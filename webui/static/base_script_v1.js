
if (customElements.get("advanced-link") === undefined) {
    customElements.define(
        "advanced-link",
        class extends HTMLElement {
            constructor() {
                super();
                this.addEventListener("advanced-toggle", (e) => {
                    const show = e.detail;
                    if (show) {
                        const query_string = Array.from(this.attributes).map((attr) => {
                            return `${attr.name}=${attr.value}`;
                        }).reduce((a, b) => {
                            return `${a}&${b}`;
                        });
                        this.innerHTML = `<a class="advanced-link" href="/advanced?${query_string}">${this.innerHTML}</a>`;
                    } else {
                        if (this.children[0].tagName === "A") {
                            this.innerHTML = this.children[0].innerHTML;
                        }
                    }
                })
            }

        }
    )
}

function utc_to_local() {
    const times = document.getElementsByClassName("time")
    for (const time of times) {
        if (time.dataset.is_local) {
            continue
        }
        const text = time.innerText
        const date = new Date(Date.parse(text.replace(" ", "T") + "Z"))
        time.innerText = `${date.getFullYear()}-${(date.getMonth() + 1).toString().padStart(2, '0')}-${date.getDate().toString().padStart(2, '0')} ${date.getHours().toString().padStart(2, '0')}:${date.getMinutes().toString().padStart(2, '0')}:${date.getSeconds().toString().padStart(2, '0')}`
        time.dataset.is_local = true
    }
}

function local_to_utc() {
    const times = document.getElementsByClassName("time")
    for (const time of times) {
        const text = time.innerText
        const date = new Date(text)
        time.innerText = `${date.getUTCFullYear()}-${(date.getUTCMonth() + 1).toString().padStart(2, '0')}-${date.getUTCDate().toString().padStart(2, '0')} ${date.getUTCHours().toString().padStart(2, '0')}:${date.getUTCMinutes().toString().padStart(2, '0')}:${date.getUTCSeconds().toString().padStart(2, '0')}`
    }
}

timezone_setting_element = document.getElementById("timezone-setting")

function toggle_timezone() {
    if (localStorage.getItem("timezone") === "local") {
        localStorage.setItem("timezone", "utc")
        timezone_setting_element.innerText = "UTC"
        local_to_utc()
    } else {
        localStorage.setItem("timezone", "local")
        timezone_setting_element.innerText = "Local"
        utc_to_local()
    }
}


timezone_setting = localStorage.getItem("timezone")
if (timezone_setting === null) {
    timezone_setting = "utc"
    localStorage.setItem("timezone", "utc")
}

function apply_timezone() {
    if (timezone_setting === "local") {
        utc_to_local()
        timezone_setting_element.innerText = "Local"
    } else {
        timezone_setting_element.innerText = "UTC"
    }
}

(() => {
    apply_timezone()
})()

advanced_setting = "false"
if (advanced_setting === null) {
    advanced_setting = false
    localStorage.setItem("advanced", "false")
} else {
    advanced_setting = JSON.parse(advanced_setting)
}

advanced_setting_element = document.getElementById("advanced-setting")

advanced_setting_element.innerText = "Off"
dispatch_advanced_event()

function dispatch_advanced_event() {
    const advanced = document.getElementsByTagName("advanced-link")
    for(const element of advanced) {
        element.dispatchEvent(new CustomEvent("advanced-toggle", { detail: advanced_setting }))
    }
}

function toggle_advanced() {
    if (advanced_setting) {
        localStorage.setItem("advanced", "false")
        advanced_setting_element.innerText = "Off"
    } else {
        localStorage.setItem("advanced", "true")
        advanced_setting_element.innerText = "Ofn"
    }
    advanced_setting = !advanced_setting
    dispatch_advanced_event()
}