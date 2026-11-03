import { HashRouter } from "react-router"

import { DataProvider } from "./client/DatatContext.js"
import { ObserverRoutes } from "./routes.js"

export function App() {
    return (
        <main className="min-h-screen px-8 pt-7 pb-8 font-satoshi antialiased">
            <DataProvider>
                <HashRouter>
                    <ObserverRoutes />
                </HashRouter>
            </DataProvider>
        </main>
    )
}
export default App
