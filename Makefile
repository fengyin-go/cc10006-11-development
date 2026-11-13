.PHONY: install backend frontend verify-checkpoint

install:
	cd backend && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
	cd frontend && npm install

backend:
	cd backend && ./run.sh

frontend:
	cd frontend && npm run dev

# 需要先起后端：验证检查点恢复、并发去重、非法水位拒绝与输出不重复
verify-checkpoint:
	cd backend && .venv/bin/python scripts/verify_checkpoint_recovery.py
