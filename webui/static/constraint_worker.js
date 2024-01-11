import init, {worker_get_constraint_numbers} from '/static/rust.js'

await init()

self.postMessage({ready: true})

self.onmessage = function(e) {
    const program = e.data
    try {
        const msg = []
        const result = worker_get_constraint_numbers(program)
        for (const f of result) {
            msg.push({function_name: f.function_name, constraint_number: f.constraint_number})
        }
        self.postMessage(msg)
    } catch (ex) {
        self.postMessage({error: ex.toString()})
    }
}

