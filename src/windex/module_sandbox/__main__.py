import uvicorn

uvicorn.run(
    "windex.module_sandbox.app:app",
    host="0.0.0.0",
    port=8110,
    access_log=False,
)
