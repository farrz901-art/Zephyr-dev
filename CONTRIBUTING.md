## 0.0.1
GitHub 官方文档解释了 core.autocrlf 的行为与选择策略。对“以 Linux 为运行时”的项目，常见建议是 Windows 开发者用 input 或显式依赖 .gitattributes 来统一
git config --global core.autocrlf input
git config --global core.safecrlf true
