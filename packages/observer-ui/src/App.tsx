import { Overview } from "./components/overview/overview.js"
import { mockOverview } from "./mocks/overview.js"
import { DataProvider } from "./client/DatatContext.js";

export function App() {
    return (
        <AppContent />
    )
}

function AppContent() {
    return (
        <main className="min-h-screen px-8 pt-7 pb-8 font-satoshi antialiased">
            <DataProvider>
                <Overview metrics={mockOverview} />
            </DataProvider>
        </main>
    )
}
export default App;
