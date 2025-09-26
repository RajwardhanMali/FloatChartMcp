from fastapi import FastAPI
import uvicorn

app = FastAPI()

@app.get("/")
async def health_check():
    return "The health check is successful!"


if __name__ == "__main__" :
    uvicorn.run(app,host="localhost",port=8000)