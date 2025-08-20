import asyncio
import random
import datetime
import re
from typing import List, Tuple, Type

from src.plugin_system import (
    BasePlugin,
    register_plugin,
    ConfigField,
    ComponentInfo,
    BaseCommand,
)
from src.plugin_system.apis import llm_api, send_api, get_logger, config_api, chat_api

async def _send_greeting_logic(logger, get_config, schedule_config: dict, override_target_qq: str = None):
    """生成并发送问候语的核心逻辑"""
    keywords = schedule_config.get("keywords", [])
    if not keywords:
        logger.warning(f"任务 '{schedule_config.get('name', '未知')}' 未配置问候关键词。")
        return False

    # 使用独立的随机实例以避免全局种子问题
    rand_instance = random.Random()
    keyword = rand_instance.choice(keywords)
    prompt_template = schedule_config.get("prompt_template", "请生成一句问候。")

    # 从全局配置中获取核心人设和侧面人设
    personality_core = config_api.get_global_config("personality.personality_core", "一个机器人")
    personality_side = config_api.get_global_config("personality.personality_side", "")

    # 将两者拼接成一个完整的人设描述
    full_persona = f"{personality_core} {personality_side}".strip()

    # 替换模板中的占位符
    prompt = prompt_template.replace("{keyword}", keyword)
    prompt = prompt.replace("{persona}", full_persona)


    models = llm_api.get_available_models()
    logger.info(f"获取LLM模型：{models}")
    model_name = get_config("plugin.llm_model_name", "replyer") # 从根配置获取
    model = models.get(model_name)

    if not model:
        logger.error(f"未找到名为 '{model_name}' 的LLM模型。请检查配置。")
        return False

    success, response, _, _ = await llm_api.generate_with_model(
        prompt=prompt,
        model_config=model,
        request_type="plugin.timed_greeting"
    )

    if not (success and response):
        logger.error("使用LLM生成问候语失败。")
        return False
    
    logger.info(f"成功生成问候语: {response}")

    # 如果是测试命令，直接发送给发起者

    # logger.debug(f"获取聊天流：{chat_api.get_all_streams()}")

    if override_target_qq:
        user_stream_id = chat_api.get_stream_by_user_id(user_id=str(override_target_qq), platform="qq").stream_id
        sent = await send_api.text_to_stream(text=response, stream_id=user_stream_id)
        if sent:
            logger.info(f"已成功向测试用户 {override_target_qq} 发送问候。")
        else:
            logger.error(f"向测试用户 {override_target_qq} 发送问候失败。")
        return sent

    # 否则，遍历所有配置的目标
    targets = get_config("targets", [])
    all_sent = True
    for target_group in targets:
        target_type = target_group.get("type")
        id_list = target_group.get("id", [])

        if not (target_type in ["private", "group"] and isinstance(id_list, list)):
            logger.warning(f"发现格式不正确的目标配置，已跳过: {target_group}")
            continue

        for target_id in id_list:
            if not target_id:
                continue

            sent = False
            try:
                if target_type == "private":
                    user_stream_id = chat_api.get_stream_by_user_id(user_id=str(target_id), platform="qq").stream_id
                    sent = await send_api.text_to_stream(text=response, stream_id=user_stream_id)
                elif target_type == "group":
                    group_stream_id = chat_api.get_stream_by_group_id(group_id=str(target_id), platform="qq").stream_id
                    sent = await send_api.text_to_stream(text=response, stream_id=group_stream_id)
                
                if sent:
                    logger.info(f"已成功向目标 {target_type}:{target_id} 发送问候。")
                else:
                    logger.error(f"向目标 {target_type}:{target_id} 发送问候失败。")
                    all_sent = False
            except Exception as e:
                logger.error(f"向目标 {target_type}:{target_id} 发送时发生异常: {e}", exc_info=True)
                all_sent = False
            
    return all_sent

class TestGreetingCommand(BaseCommand):
    """手动测试发送问候语的命令"""
    command_name = "test_greeting"
    command_description = "手动触发一次问候语的生成和发送。用法: /test_greeting <schedule_name>"
    command_pattern = r"^/test_greeting\s+(?P<schedule_name>\w+)$"
    command_examples = ["/test_greeting morning_greeting", "/test_greeting evening_greeting"]

    async def execute(self) -> Tuple[bool, str, bool]:
        """执行手动问候"""
        # 权限检查
        admin_qqs = [str(admin_id) for admin_id in self.get_config("plugin.admin_qqs", [])]
        user_id = self.message.message_info.user_info.user_id
        is_group_message = self.message.message_info.group_info is not None

        if user_id not in admin_qqs:
            if is_group_message:
                # 在群聊中，非管理员使用直接静默失败，不返回任何消息
                return False, "权限不足", True
            else:
                # 在私聊中，提示权限不足
                await self.send_text("您没有权限使用此命令。")
                return False, "权限不足", True

        schedule_name = self.matched_groups.get("schedule_name")
        schedules = self.get_config("schedules", {})
        schedule_config = schedules.get(schedule_name)

        if not schedule_config:
            await self.send_text(f"未找到名为 '{schedule_name}' 的计划任务。可用的任务有: {', '.join(schedules.keys())}")
            return False, "未找到计划任务", True

        await self.send_text(f"正在手动触发 '{schedule_name}' 任务...")
        logger = get_logger("timed_greeting_plugin_manual")
        
        # 为任务配置添加名称以便日志记录
        schedule_config['name'] = schedule_name
        
        # 调用核心逻辑，并将目标QQ覆盖为发送此命令的用户ID，以便测试
        user_id = self.message.message_info.user_info.user_id
        success = await _send_greeting_logic(logger, self.get_config, schedule_config, override_target_qq=user_id)
        if success:
            return True, "手动触发完成。", False
        else:
            return False, "发送失败，请检查日志。", True

class TimedGreetingSender:
    """
    采用持续调度模型的定时问候发送器。
    任务执行后会立即计划下一次运行，而不是每日重置。
    """

    def __init__(self, plugin):
        self.plugin = plugin
        self.is_running = False
        self.task = None
        self.logger = get_logger("TimedGreetingSender")
        self._scheduled_times = {}  # 任务名 -> 下一个 datetime 执行时间
        self.rand_instance = random.Random()

    def _calculate_next_schedule(self, name: str, config: dict, is_reschedule: bool = False) -> datetime.datetime | None:
        """
        计算单个任务的下一个有效执行时间。
        通过 is_reschedule 标志区分初次调度和任务执行后的重新调度，根治“一晚双发”问题。
        """
        try:
            now = datetime.datetime.now()
            
            start_time = datetime.datetime.strptime(config.get("start_time", "00:00"), "%H:%M").time()
            end_time = datetime.datetime.strptime(config.get("end_time", "23:59"), "%H:%M").time()
            is_exact_time = start_time == end_time
            is_cross_day = end_time < start_time

            # 1. 确定目标逻辑日的开始日期
            #    - 对于重调度，总是跳到下一个逻辑日。
            #    - 对于初始调度，尝试使用当前逻辑日。

            # 找到当前时间所在的逻辑日的开始日期
            current_logical_day_start_date = now.date()
            if is_cross_day and now.time() < end_time:
                current_logical_day_start_date -= datetime.timedelta(days=1)

            target_day_start_date = current_logical_day_start_date

            # 如果是重新调度，则目标日期必须是下一个逻辑日
            if is_reschedule:
                target_day_start_date += datetime.timedelta(days=1)

            # 2. 计算目标逻辑日的时间窗口
            window_start = datetime.datetime.combine(target_day_start_date, start_time)
            window_end = datetime.datetime.combine(target_day_start_date, end_time)
            if is_cross_day:
                window_end += datetime.timedelta(days=1)

            # 3. 如果目标窗口完全过去，则强制跳到下一个逻辑日（主要用于初始启动时处理已过时的非跨天任务）
            if now > window_end:
                target_day_start_date += datetime.timedelta(days=1)
                window_start = datetime.datetime.combine(target_day_start_date, start_time)
                window_end = datetime.datetime.combine(target_day_start_date, end_time)
                if is_cross_day:
                    window_end += datetime.timedelta(days=1)

            # 4. 根据任务类型（精确时间 vs. 时间范围）计算计划时间
            if is_exact_time:
                # 精确时间任务，无需随机偏移
                scheduled_datetime = window_start
            else:
                # 时间范围任务，计算随机偏移
                time_diff_seconds = (window_end - window_start).total_seconds()
                if time_diff_seconds < 0: # 小于0是无效的，等于0是精确时间
                    self.logger.warning(
                        f"任务 '{name}' 的时间范围无效 (start: {start_time.strftime('%H:%M')}, end: {end_time.strftime('%H:%M')})。"
                        "对于非跨天任务, start_time 必须早于 end_time。已跳过此任务。"
                    )
                    return None
                
                random_offset = self.rand_instance.randint(0, int(time_diff_seconds))
                scheduled_datetime = window_start + datetime.timedelta(seconds=random_offset)

            # 5. 最终防卫：如果计算出的时间仍然在过去（例如，初始调度时，随机到了今天已过的时间），
            #    则强制进行一次“重新调度”，以获取下一个周期的有效时间。
            if scheduled_datetime < now:
                self.logger.debug(f"任务 '{name}' 在目标周期 ({target_day_start_date}) 的随机时间 ({scheduled_datetime.strftime('%H:%M:%S')}) 已过，强制计划下一个周期。")
                return self._calculate_next_schedule(name, config, is_reschedule=True)

            return scheduled_datetime

        except (ValueError, TypeError) as e:
            self.logger.error(f"解析任务 '{name}' 的时间配置时出错: {e}。")
            return None
        except Exception as e:
            self.logger.error(f"计算任务 '{name}' 的下一次执行时间时发生未知错误: {e}", exc_info=True)
            return None

    def _schedule_all_tasks_initial(self):
        """在启动时为所有已启用的任务计算并设置初始计划。"""
        self.logger.info("正在进行首次任务规划...")
        schedules = self.plugin.get_config("schedules", {})
        self._scheduled_times.clear()
        for name, config in schedules.items():
            if not config.get("enabled", False):
                continue
            
            next_run_time = self._calculate_next_schedule(name, config, is_reschedule=False)
            if next_run_time:
                self._scheduled_times[name] = next_run_time
                self.logger.info(f"任务 '{name}' 已安排。下次执行时间: {next_run_time.strftime('%Y-%m-%d %H:%M:%S')}")

    async def _execute_and_reschedule_task(self, name: str):
        """执行单个任务，然后立即为其重新安排下一次执行。"""
        scheduled_time = self._scheduled_times.get(name)
        self.logger.info(f"任务 '{name}' 已到达计划时间 ({scheduled_time.strftime('%Y-%m-%d %H:%M:%S') if scheduled_time else 'N/A'})，准备发送。")
        
        config = self.plugin.get_config("schedules", {}).get(name, {})
        config['name'] = name
        
        await _send_greeting_logic(self.logger, self.plugin.get_config, config)
        self.logger.info(f"任务 '{name}' 本次执行完毕。")

        # 立即重新规划
        if config.get("enabled", False):
            # 重新规划时，强制指定 is_reschedule=True
            next_run_time = self._calculate_next_schedule(name, config, is_reschedule=True)
            if next_run_time:
                self._scheduled_times[name] = next_run_time
                self.logger.info(f"任务 '{name}' 已重新安排。下次执行时间: {next_run_time.strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                # 如果无法计算下一次时间（例如，配置错误），则从调度中移除
                if name in self._scheduled_times:
                    del self._scheduled_times[name]
                self.logger.warning(f"无法为任务 '{name}' 计算下一次执行时间，已将其从调度中移除。")
        else:
            # 如果任务已被禁用，则从调度中移除
            if name in self._scheduled_times:
                del self._scheduled_times[name]
            self.logger.info(f"任务 '{name}' 已被禁用，将不再调度。")

    async def start(self):
        """启动定时发送任务"""
        if self.is_running:
            return
        self.is_running = True
        self.task = asyncio.create_task(self._schedule_loop())
        self.logger.info("多功能定时任务已启动")

    async def stop(self):
        """停止定时发送任务"""
        if not self.is_running:
            return
        self.is_running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        self.logger.info("多功能定时任务已停止")

    async def _schedule_loop(self):
        """持续调度循环"""
        self.logger.info("开始持续任务调度循环。")
        self._schedule_all_tasks_initial()
        
        while self.is_running:
            try:
                if not self._scheduled_times:
                    # 如果当前没有计划任务（可能都禁用了），则等待一段时间后重新检查
                    await asyncio.sleep(60)
                    self._schedule_all_tasks_initial()
                    continue

                now = datetime.datetime.now()
                
                # 寻找下一个最近的到期任务
                # 使用 list() 来避免在迭代时修改字典
                tasks_to_run = [
                    (name, scheduled_time)
                    for name, scheduled_time in self._scheduled_times.items()
                    if now >= scheduled_time
                ]

                for name, _ in tasks_to_run:
                    await self._execute_and_reschedule_task(name)

                # --- 动态计算休眠时间 ---
                if self._scheduled_times:
                    next_task_time = min(self._scheduled_times.values())
                    sleep_seconds = (next_task_time - datetime.datetime.now()).total_seconds()
                    # 确保至少休眠1秒，避免CPU空转；最长休眠60秒，以响应配置变化
                    sleep_seconds = max(1, min(sleep_seconds, 60))
                else:
                    sleep_seconds = 60 # 没有任务了，一分钟后检查

                await asyncio.sleep(sleep_seconds)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"调度循环出现严重错误: {e}", exc_info=True)
                await asyncio.sleep(60) # 发生错误后等待一段时间再重试

@register_plugin
class TimedGreetingPlugin(BasePlugin):
    """定时问候插件"""

    plugin_name = "timed_greeting_plugin"
    
    enable_plugin = True
    dependencies = []
    python_dependencies = []
    config_file_name = "config.toml"

    config_section_descriptions = {
        "plugin": "插件基础配置",
        "targets": "发送目标列表",
        "schedules": "定时任务计划"
    }

    config_schema = {
        "plugin": {
            "enabled": ConfigField(type=bool, default=True, description="是否启用插件"),
            "config_version": ConfigField(type=str, default="0.2.0", description="配置文件版本"),
            "admin_qqs": ConfigField(
                type=list,
                default=[],
                description="管理员QQ号列表，用于接收错误通知和使用 /test_greeting 命令。"
            ),
            "llm_model_name": ConfigField(type=str, default="replyer_1", description="使用的LLM模型名称"),
        },
        "targets": ConfigField(
            type=list,
            default=[
                {"type": "private", "id": []},
                {"type": "group", "id": []}
            ],
            description="发送目标的配置列表。"
        ),
        "schedules": ConfigField(
            type=dict,
            default={
                "morning_greeting": {
                    "enabled": True,
                    "start_time": "08:00",
                    "end_time": "09:00",
                    "keywords": ["早安", "早上好", "元气满满"],
                    "prompt_template": "请根据关键词 '{keyword}' 生成一句简短、友好、自然的早安问候语。请只返回一句话，不要添加任何编号或多余的解释。"
                },
                "evening_greeting": {
                    "enabled": True,
                    "start_time": "22:00",
                    "end_time": "23:00",
                    "keywords": ["晚安", "睡个好觉", "做个好梦"],
                    "prompt_template": "我的设定是'{persona}'。现在请你根据关键词'{keyword}'，以我的身份生成一句简短、温馨、自然的晚安问候语。请只返回一句话，不要添加任何编号或多余的解释。"
                }
            },
            description="定时任务的详细配置"
        )
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.scheduler = None
        self.logger = get_logger("TimedGreetingPlugin")

        # 启动时校验
        if not self._validate_and_setup_scheduler():
            return # 如果校验失败，则不启动任何任务

    def _validate_and_setup_scheduler(self) -> bool:
        """校验配置并根据结果设置调度器，返回是否成功"""
        schedules = self.get_config("schedules", {})
        invalid_names = []
        
        # 校验任务名合法性
        valid_name_pattern = re.compile(r"^[a-zA-Z_]+$")
        for name in schedules.keys():
            if not valid_name_pattern.match(name):
                invalid_names.append(name)

        if invalid_names:
            error_message = (
                f"定时问候插件启动失败：发现不合法的任务名。\n"
                f"任务名只能包含大小写字母和下划线 (_)。\n"
                f"非法任务名列表: {', '.join(invalid_names)}"
            )
            self.logger.error(error_message)
            
            # 异步通知管理员
            asyncio.create_task(self._notify_admin_of_error(error_message))
            return False # 校验失败

        # 校验发送目标和管理员配置
        targets = self.get_config("targets", [])
        admin_qqs = self.get_config("plugin.admin_qqs", [])
        
        has_any_target_id = any(
            id_val
            for target_group in targets
            if isinstance(target_group, dict) and target_group.get("id")
            for id_val in target_group.get("id", [])
        )

        if not has_any_target_id:
            if admin_qqs:
                # 仅配置了管理员，进入“仅命令”模式
                self.logger.info("仅配置了管理员QQ，未配置任何发送目标。定时任务已禁用，仅 /test_greeting 命令可用。")
                # 不创建 scheduler，但返回 True 以加载命令
                return True
            else:
                # 管理员和发送目标均未配置，插件无事可做
                self.logger.warning("管理员和发送目标均未配置，插件已停用。")
                return False

        # 所有校验通过，启动调度器
        self.scheduler = TimedGreetingSender(self)
        asyncio.create_task(self._start_scheduler_after_delay())
        return True

    async def _notify_admin_of_error(self, message: str):
        """延迟并尝试向管理员发送错误通知"""
        await asyncio.sleep(30) # 延迟30秒，等待Bot连接稳定
        
        admin_qqs = self.get_config("plugin.admin_qqs", [])
        if not admin_qqs:
            self.logger.warning("未配置管理员QQ，无法发送启动错误通知。错误已记录在日志中。")
            return

        for admin_id in admin_qqs:
            try:
                success = await send_api.text_to_user(text=message, user_id=str(admin_id), platform="qq")
                if success:
                    self.logger.info(f"已向管理员 {admin_id} 发送配置错误通知。")
                else:
                    self.logger.error(f"向管理员 {admin_id} 发送配置错误通知失败。")
            except Exception as e:
                self.logger.error(f"向管理员 {admin_id} 发送通知时发生异常: {e}", exc_info=True)

    async def _start_scheduler_after_delay(self):
        """延迟启动日程任务"""
        await asyncio.sleep(10) # 等待插件完全初始化
        if self.scheduler:
            await self.scheduler.start()

    def get_plugin_components(self) -> List[Tuple[ComponentInfo, Type]]:
        """返回插件包含的组件列表"""
        return [
            (TestGreetingCommand.get_command_info(), TestGreetingCommand)
        ]
