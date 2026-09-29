import { createRoot } from "react-dom/client"

import { App } from "./App.js"
import { PracticeApi } from "./client.js"
import "./style.css"

createRoot(document.getElementById("root")!).render(<App api={new PracticeApi((url, options) => fetch(url, options))} />)
