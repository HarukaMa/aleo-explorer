import asyncio
import decimal

from dotenv import load_dotenv

from explorer import Explorer
from util.set_proc_title import set_proc_title

load_dotenv()

async def main():
    set_proc_title("aleo-explorer: main")
    decimal.getcontext().prec = 80
    e = Explorer()
    e.start()
    while True:
        await asyncio.sleep(3600)

if __name__ == '__main__':
    asyncio.run(main())