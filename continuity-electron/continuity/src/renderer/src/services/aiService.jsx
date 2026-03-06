const API_BASE = "http://localhost:8002";

export async function extractEntities(text) {

    const start = await fetch(`${API_BASE}/entities/extract/start`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
            text,
            time_id: "t_001"
        })
    });

    const { jobId } = await start.json();

    let status = "running";

    while (status !== "done") {

        await new Promise(r => setTimeout(r, 500));

        const res = await fetch(`${API_BASE}/entities/status/${jobId}`);
        const data = await res.json();

        status = data.status;

        if (status === "error") {
            throw new Error(data.message);
        }
    }

    const resultRes = await fetch(`${API_BASE}/entities/result/${jobId}`);
    const result = await resultRes.json();

    // 🔹 Normalize backend format into UI format
    return {
        ...result,
        entities: result.entities.map(entity => ({
            id: entity.id,
            name: entity.name,
            type: entity.entityType,
            aliases: entity.aliases || [],

            facts: (entity.facts || []).map((fact, index) => ({
                id: `${entity.id}-fact-${index}`,
                text: fact.fact,
                accepted: null,
                confidence: fact.confidence,
                sourceText: fact.sourceText
            }))
        }))
    }
}