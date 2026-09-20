import { createRoot } from "react-dom/client"

import { ConsoleApp } from "./ConsoleApp.js"
import { HttpObserverClient } from "./client.js"
import "./standalone.css"

const client = new HttpObserverClient()
const darkMode = window.matchMedia("(prefers-color-scheme: dark)")
let manualTheme = false
const updateTheme = () => {
    if (!manualTheme) document.documentElement.classList.toggle("dark", darkMode.matches)
}
updateTheme()
darkMode.addEventListener("change", updateTheme)
function toggleTheme() {
    manualTheme = true
    document.documentElement.classList.toggle("dark")
}
createRoot(document.getElementById("root")!).render(<ConsoleApp client={client} toggleTheme={toggleTheme} />)
