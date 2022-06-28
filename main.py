import asyncio

from dotenv import load_dotenv

from explorer import Explorer

load_dotenv()

async def main():
    e = Explorer()
    e.start()
    while True:
        await asyncio.sleep(3600)

if __name__ == '__main__':
    asyncio.run(main())