const API_BASE = "http://localhost:8001";

function normalizeResult(result, fallbackText) {
    return {
        ...result,
        summary: (fallbackText || "").slice(0, 140) + ((fallbackText || "").length > 140 ? "..." : ""),
        entitySummary: result.summary || [],
        crossStoryConflictsCount: result.crossStoryConflictsCount || 0,
        entities: (result.entities || []).map((entity) => ({
            id: entity.id,
            name: entity.name,
            type: entity.entityType,
            aliases: entity.aliases || [],
            facts: (entity.facts || []).map((fact, index) => ({
                id: fact.id || `${entity.id}-fact-${index}`,
                text: fact.fact,
                entityId: fact.entity_id,
                accepted: fact.status === "approved" ? true : fact.status === "rejected" ? false : null,
                confidence: fact.confidence,
                sourceText: fact.sourceText,
                status: fact.status,
                conflictGroupId: fact.conflict_group_id,
                contradicts: fact.contradicts || [],
                matchConfidence: fact.entity_match_confidence,
                matchAmbiguous: fact.entity_match_ambiguous,
                matchCandidates: fact.entity_match_candidates || [],
                assignmentConfirmed: fact.entity_assignment_confirmed,
                atomicityScore: fact.atomicity_score,
                schemaAlignmentScore: fact.schema_alignment_score,
                needsReview: fact.needs_review,
            })),
        })),
    };
}

export async function uploadSegment(projectId, text, title) {
    const res = await fetch(`${API_BASE}/projects/${projectId}/segments`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ body: text, title }),
    });

    if (!res.ok) {
        let message = `Segment upload failed (${res.status})`;
        try {
            const data = await res.json();
            message = data.detail || message;
        } catch {
            // Ignore parse errors and use generic message.
        }
        throw new Error(message);
    }

    const data = await res.json();
    return normalizeResult(data, text);
}

export async function extractEntities(text) {
    // Temporary compatibility wrapper until all callers pass project id.
    throw new Error("extractEntities(text) is deprecated. Use uploadSegment(projectId, text, title).");
}

export async function reviewFact(factId, status, reviewedBy, decisionReason, confirmAssignment = false, confirmLowQuality = false) {
    const res = await fetch(`${API_BASE}/facts/${factId}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
            status,
            reviewed_by: reviewedBy,
            decision_reason: decisionReason,
            confirm_assignment: Boolean(confirmAssignment),
            confirm_low_quality: Boolean(confirmLowQuality),
        }),
    });
    if (!res.ok) {
        let message = `Fact review failed (${res.status})`;
        try {
            const data = await res.json();
            message = data.detail || data.message || message;
        } catch {
            // Ignore parse errors and use generic message.
        }
        throw new Error(message);
    }
    return res.json();
}

export async function assignFactEntity(factId, entityId) {
    const res = await fetch(`${API_BASE}/facts/${factId}/entity`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ entity_id: entityId }),
    });
    if (!res.ok) {
        let message = `Fact entity assignment failed (${res.status})`;
        try {
            const data = await res.json();
            message = data.detail || message;
        } catch {
            // Ignore parse errors and use generic message.
        }
        throw new Error(message);
    }
    return res.json();
}

export async function submitReviewSession(sessionId, submittedBy) {
    const res = await fetch(`${API_BASE}/review-sessions/${sessionId}/submit`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ submitted_by: submittedBy }),
    });
    if (!res.ok) {
        let message = `Submit review failed (${res.status})`;
        try {
            const data = await res.json();
            message = data.detail || message;
        } catch {
            // Ignore parse errors and use generic message.
        }
        throw new Error(message);
    }
    return res.json();
}