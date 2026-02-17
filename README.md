# astrbot_plugin_campus_alert

基于 AstrBot 的校园告警邮箱监控插件。

## 使用前提
- 需要学校提供校园告警邮件服务。
- 需要一个专门的邮箱账号（如 Gmail）用于接收校园告警邮件，并开启 IMAP 功能。
- 需要在 AstrBot 插件配置中正确设置邮箱的 IMAP 服务器地址

## 功能

- 通过 IMAP 轮询邮箱并解析邮件
- 仅处理允许的发件人并执行 24 小时窗口过滤
- 调用 AstrBot LLM 进行告警判定和摘要
- 去重持久化（`processed_emails.json`）
- 向配置的统一会话 ID 列表推送告警

## 运维指令

- `/campusalert status`
- `/campusalert checknow`
- `/campusalert reload`
