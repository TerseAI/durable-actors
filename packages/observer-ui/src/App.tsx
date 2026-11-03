import { StatCard, StatCardProps } from "./components/ui/statCard.js"

const stats: StatCardProps[] = [
    {
        title: "Durable Actors",
        description: "Durable Actors are a way to store state in a distributed system."
    },
    {
        title: "Durable Actors",
        description: "Durable Actors are a way to store state in a distributed system."
    },
    
]

export function App() {
    return (
        <main className="flex min-h-screen flex-col gap-4 p-6">
            <div className="flex flex-col gap-4">
                {stats.map((stat) => (
                    <StatCard key={stat.title} {...stat} />
                ))}
            </div>
        </main>
    )
}
export default App;
