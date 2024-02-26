import asyncio
import decimal
import gc

from dotenv import load_dotenv

from explorer import Explorer
from util.set_proc_title import set_proc_title

load_dotenv()

async def main():
    # https://mkennedy.codes/posts/python-gc-settings-change-this-and-make-your-app-go-20pc-faster/
    # Clean up what might be garbage so far.
    gc.collect(2)
    # Exclude current items from future GC.
    gc.freeze()

    allocs, gen1, gen2 = gc.get_threshold()
    allocs = 200_000  # Start the GC sequence every 200K not 700 allocations.
    gen1 = gen1 * 5
    gen2 = gen2 * 5
    gc.set_threshold(allocs, gen1, gen2)

    set_proc_title("aleo-explorer: main")
    decimal.getcontext().prec = 80
    e = Explorer()
    e.start()
    while True:
        await asyncio.sleep(3600)

if __name__ == '__main__':
    asyncio.run(main())