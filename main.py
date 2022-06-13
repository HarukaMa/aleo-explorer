import asyncio

from explorer import Explorer


async def main():
    e = Explorer()
    e.start()
    while True:
        await asyncio.sleep(3600)

if __name__ == '__main__':
    asyncio.run(main())