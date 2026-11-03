import { StatCard, StatCardProps } from "./components/ui/statCard.js"
import { useEffect } from "react";
import { HttpObserverClient } from "./client/observerClient.js";

const stats: StatCardProps[] = [
    { label: "Active actors", value: "1,284", detail: "across 12 classes" },
    { label: "Requests / min", value: "8,412", detail: "last hour" },
    { label: "p95 latency", value: "42 ms", detail: "last hour" },
    { label: "Open WebSocket connections", value: 316 }
]

export function App() {

    useEffect(() => {
        const client = new HttpObserverClient();
        client.listActorTypes().then(inventory => {
            console.log(inventory);
        });
    }, []);
    
    return (
        <main className="flex min-h-screen flex-col gap-4 p-6">
            <dl className="grid grid-cols-2 gap-3 lg:grid-cols-4">
                {stats.map(stat => (
                    <StatCard key={stat.label} {...stat} />
                ))}
            </dl>
        </main>
    )
}
export default App;
