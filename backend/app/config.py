"""运行配置：端口、跨域、运行环境。"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    app_name: str = "数据湖仓一体管理平台"
    env: str = "local"
    port: int = 8000
    allowed_origins: list[str] = field(
        default_factory=lambda: [
            "http://127.0.0.1:5173",
            "http://localhost:5173",
        ]
    )
    page_size_default: int = 20
    page_size_max: int = 200
    # 检查点持久化文件；留空时用 backend/var/checkpoints.json。
    # 回归验证可以指向临时文件，避免污染本地演示数据。
    checkpoint_state_path: str = ""


settings = Settings(checkpoint_state_path=os.environ.get("CHECKPOINT_STATE_PATH", ""))
