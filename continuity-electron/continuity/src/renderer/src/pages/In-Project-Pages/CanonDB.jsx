
import { useEffect, useMemo, useState } from "react";
import projectService from "../../services/projectService";
import { reviewFact } from "../../services/aiService";

const API_BASE = "http://localhost:8001";

const emptyForm = {
  name: "",
  type: "concept",
  aliases: "",
  description: "",
  notes: "",
};

const ENTITY_TYPE_OPTIONS = [
  { label: "Character", value: "character" },
  { label: "Location", value: "location" },
  { label: "Organization", value: "organization" },
  { label: "Event", value: "event" },
  { label: "Concept", value: "concept" },
  { label: "Item", value: "item" },
  { label: "Creature", value: "creature" },
];

const CanonDB = () => {
  const [project, setProject] = useState(null);
  const [entities, setEntities] = useState([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState(null);
  const [query, setQuery] = useState("");
  const [form, setForm] = useState(emptyForm);
  const [editingId, setEditingId] = useState(null);
  const [expandedEntityId, setExpandedEntityId] = useState(null);
  const [mergeSourceId, setMergeSourceId] = useState("");
  const [mergeTargetId, setMergeTargetId] = useState("");
  const [syncStatus, setSyncStatus] = useState({ pendingCount: 0, errorCount: 0, totalSubmitted: 0, sessions: [] });
  const [syncMessage, setSyncMessage] = useState("");
  const [suggestedEntities, setSuggestedEntities] = useState([]);

  useEffect(() => {
    let mounted = true;

    const load = async () => {
      try {
        const currentProject = await projectService.loadProject();
        if (!mounted) return;
        setProject(currentProject);
        if (currentProject?.id) {
          await refreshEntities(currentProject.id, "", mounted);
          await refreshSuggestedEntities(currentProject.id, mounted);
          await refreshSyncStatus(currentProject.id, mounted);
        }
      } catch (err) {
        if (mounted) {
          setError(err.message || "Failed to load Canon data");
        }
      } finally {
        if (mounted) setLoading(false);
      }
    };

    load();

    return () => {
      mounted = false;
    };
  }, []);

  const refreshEntities = async (projectId, nextQuery = query, mounted = true) => {
    const params = new URLSearchParams();
    if (nextQuery.trim()) params.set("query", nextQuery.trim());
    const res = await fetch(`${API_BASE}/projects/${projectId}/canon/entities${params.toString() ? `?${params.toString()}` : ""}`);
    if (!res.ok) {
      throw new Error(`Failed to load Canon entities (${res.status})`);
    }
    const data = await res.json();
    if (mounted) {
      setEntities(data.entities || []);
    }
  };

  const refreshSuggestedEntities = async (projectId, mounted = true) => {
    const res = await fetch(`${API_BASE}/projects/${projectId}/canon/suggestions`);
    if (!res.ok) {
      throw new Error(`Failed to load suggested entities (${res.status})`);
    }
    const data = await res.json();
    if (mounted) {
      setSuggestedEntities(data.entities || []);
    }
  };

  const refreshSyncStatus = async (projectId, mounted = true) => {
    const res = await fetch(`${API_BASE}/projects/${projectId}/canon/sync-status`);
    if (!res.ok) {
      throw new Error(`Failed to load sync status (${res.status})`);
    }
    const data = await res.json();
    if (mounted) {
      setSyncStatus({
        pendingCount: data.pendingCount || 0,
        errorCount: data.errorCount || 0,
        totalSubmitted: data.totalSubmitted || 0,
        sessions: data.sessions || [],
      });
    }
  };

  const sortedEntities = useMemo(() => {
    return [...entities].sort((a, b) => (a.name || "").localeCompare(b.name || ""));
  }, [entities]);

  const formatFactMeta = (fact) => {
    const pieces = [];
    if (fact.status) pieces.push(`status: ${fact.status}`);
    if (typeof fact.confidence === "number") pieces.push(`confidence: ${Math.round(fact.confidence * 100)}%`);
    if (typeof fact.atomicity_score === "number") pieces.push(`atomicity: ${Math.round(fact.atomicity_score * 100)}%`);
    if (typeof fact.schema_alignment_score === "number") pieces.push(`schema: ${Math.round(fact.schema_alignment_score * 100)}%`);
    if (fact.needs_review) pieces.push("needs review");
    if (fact.entity_match_ambiguous) pieces.push("ambiguous");
    if (fact.entity_assignment_confirmed) pieces.push("assignment confirmed");
    return pieces.join(" · ");
  };

  const handleFactReview = async (entityId, fact, nextStatus) => {
    if (!project?.id || !fact?.id) return;
    setSaving(true);
    setError(null);
    try {
      const isLowQuality = Boolean(
        fact && (
          fact.needs_review ||
          (typeof fact.atomicity_score === "number" && fact.atomicity_score < 0.75) ||
          (typeof fact.schema_alignment_score === "number" && fact.schema_alignment_score < 0.55)
        )
      );

      await reviewFact(
        fact.id,
        nextStatus,
        null,
        isLowQuality ? "Canon entity review" : null,
        Boolean(fact.entity_match_ambiguous && !fact.entity_assignment_confirmed),
        isLowQuality && nextStatus === "approved",
      );

      await refreshEntities(project.id, query);
      await refreshSyncStatus(project.id);
      setExpandedEntityId(entityId);
    } catch (err) {
      setError(err.message || `Failed to ${nextStatus} fact`);
    } finally {
      setSaving(false);
    }
  };

  const handleSearch = async (event) => {
    event.preventDefault();
    if (!project?.id) return;
    setLoading(true);
    setError(null);
    try {
      await refreshEntities(project.id, query);
    } catch (err) {
      setError(err.message || "Search failed");
    } finally {
      setLoading(false);
    }
  };

  const handleSubmit = async (event) => {
    event.preventDefault();
    if (!project?.id || !form.name.trim()) return;

    setSaving(true);
    setError(null);

    const payload = {
      name: form.name.trim(),
      type: form.type.trim() || "concept",
      aliases: form.aliases
        .split(/[\n,]/)
        .map((value) => value.trim())
        .filter(Boolean),
      description: form.description.trim() || null,
      notes: form.notes.trim() || null,
      confidence: 1,
    };

    try {
      const res = await fetch(
        editingId ? `${API_BASE}/entities/${editingId}` : `${API_BASE}/projects/${project.id}/canon/entities`,
        {
          method: editingId ? "PUT" : "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        }
      );

      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.detail || `Save failed (${res.status})`);
      }

      await refreshEntities(project.id);
  await refreshSuggestedEntities(project.id);
      await refreshSyncStatus(project.id);
      setForm(emptyForm);
      setEditingId(null);
        setExpandedEntityId(null);
    } catch (err) {
      setError(err.message || "Save failed");
    } finally {
      setSaving(false);
    }
  };

  const handleEdit = (entity) => {
    setEditingId(entity.id);
    setExpandedEntityId(entity.id);
    setForm({
      name: entity.name || "",
      type: entity.entityType || entity.type || "concept",
      aliases: (entity.aliases || []).join("\n"),
      description: entity.description || "",
      notes: entity.notes || "",
    });
  };

  const handlePromoteSuggestion = async (entity) => {
    if (!project?.id) return;
    setSaving(true);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/projects/${project.id}/canon/entities/${entity.id}/promote`, { method: "POST" });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.detail || `Promote failed (${res.status})`);
      }
      const data = await res.json();
      setForm({
        name: data.entity?.name || entity.name || "",
        type: data.entity?.entityType || entity.entityType || "concept",
        aliases: (data.entity?.aliases || entity.aliases || []).join("\n"),
        description: data.entity?.description || entity.description || "",
        notes: data.entity?.notes || entity.notes || "",
      });
      await refreshEntities(project.id);
      await refreshSuggestedEntities(project.id);
      await refreshSyncStatus(project.id);
    } catch (err) {
      setError(err.message || "Promote failed");
    } finally {
      setSaving(false);
    }
  };

  const handleRejectSuggestion = async (entity) => {
    if (!project?.id) return;
    setSaving(true);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/entities/${entity.id}`, { method: "DELETE" });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.detail || `Reject failed (${res.status})`);
      }
      await refreshSuggestedEntities(project.id);
    } catch (err) {
      setError(err.message || "Reject failed");
    } finally {
      setSaving(false);
    }
  };

  const handleDelete = async (entityId) => {
    if (!project?.id) return;
    setSaving(true);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/entities/${entityId}`, { method: "DELETE" });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.detail || `Delete failed (${res.status})`);
      }
      await refreshEntities(project.id);
      await refreshSuggestedEntities(project.id);
      await refreshSyncStatus(project.id);
    } catch (err) {
      setError(err.message || "Delete failed");
    } finally {
      setSaving(false);
    }
  };

  const handleMerge = async (event) => {
    event.preventDefault();
    if (!project?.id || !mergeSourceId || !mergeTargetId || mergeSourceId === mergeTargetId) return;

    setSaving(true);
    setError(null);

    try {
      const res = await fetch(`${API_BASE}/projects/${project.id}/canon/entities/merge`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ source_entity_id: mergeSourceId, target_entity_id: mergeTargetId }),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.detail || `Merge failed (${res.status})`);
      }
      await refreshEntities(project.id);
      await refreshSuggestedEntities(project.id);
      await refreshSyncStatus(project.id);
      setMergeSourceId("");
      setMergeTargetId("");
    } catch (err) {
      setError(err.message || "Merge failed");
    } finally {
      setSaving(false);
    }
  };

  const handleRetrySync = async () => {
    if (!project?.id) return;
    setSaving(true);
    setError(null);
    setSyncMessage("");
    try {
      const res = await fetch(`${API_BASE}/projects/${project.id}/canon/resync-pending`, { method: "POST" });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.detail || `Retry failed (${res.status})`);
      }
      const data = await res.json();
      setSyncMessage(`Retried ${data.retryCount || 0} sessions; ${data.successCount || 0} succeeded.`);
      await refreshSyncStatus(project.id);
    } catch (err) {
      setError(err.message || "Retry failed");
    } finally {
      setSaving(false);
    }
  };

  if (loading) {
    return <div className="canon-db">Loading Canon Database…</div>;
  }

  if (!project?.id) {
    return (
      <div className="canon-db">
        <h2>Canon Database</h2>
        <p>No active project is selected yet.</p>
      </div>
    );
  }

  return (
    <div className="canon-db" style={{ padding: "1.5rem", display: "grid", gap: "1.25rem" }}>
      <div>
        <h2 style={{ marginBottom: "0.25rem" }}>Canon Database</h2>
        <p style={{ margin: 0, opacity: 0.8 }}>Project: {project.name || project.id}</p>
      </div>

      <div style={{ display: "grid", gap: "0.5rem", padding: "1rem", border: "1px solid rgba(255,255,255,0.12)", borderRadius: "12px" }}>
        <strong>Vector Sync Status</strong>
        <div style={{ opacity: 0.85 }}>
          Submitted Sessions: {syncStatus.totalSubmitted} · Pending: {syncStatus.pendingCount} · Errors: {syncStatus.errorCount}
        </div>
        <div style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap" }}>
          <button type="button" onClick={handleRetrySync} disabled={saving || (syncStatus.pendingCount + syncStatus.errorCount) === 0}>
            Retry Pending Sync
          </button>
          {syncMessage ? <span style={{ opacity: 0.85 }}>{syncMessage}</span> : null}
        </div>
      </div>

      {error && <div style={{ padding: "0.75rem", background: "#3a1d1d", color: "#ffd7d7", borderRadius: "8px" }}>{error}</div>}

      <form onSubmit={handleSearch} style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap" }}>
        <input
          type="text"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="Search Canon entities"
          style={{ minWidth: "260px", flex: "1" }}
        />
        <button type="submit" disabled={loading || saving}>Search</button>
      </form>

      <form onSubmit={handleSubmit} style={{ display: "grid", gap: "0.75rem", padding: "1rem", border: "1px solid rgba(255,255,255,0.12)", borderRadius: "12px" }}>
        <strong>{editingId ? "Edit Entity" : "Create Entity"}</strong>
        <div style={{ display: "grid", gap: "0.5rem", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))" }}>
          <input required type="text" value={form.name} onChange={(event) => setForm((prev) => ({ ...prev, name: event.target.value }))} placeholder="Name" />
          <select value={form.type} onChange={(event) => setForm((prev) => ({ ...prev, type: event.target.value }))}>
            {ENTITY_TYPE_OPTIONS.map((option) => (
              <option key={option.value} value={option.value}>{option.label}</option>
            ))}
          </select>
        </div>
        <textarea
          value={form.aliases}
          onChange={(event) => setForm((prev) => ({ ...prev, aliases: event.target.value }))}
          placeholder="Aliases, one per line or comma separated"
          rows={3}
        />
        <small style={{ opacity: 0.75 }}>Use one alias per line, or separate multiple aliases with commas.</small>
        <textarea value={form.description} onChange={(event) => setForm((prev) => ({ ...prev, description: event.target.value }))} placeholder="Description" rows={3} />
        <textarea value={form.notes} onChange={(event) => setForm((prev) => ({ ...prev, notes: event.target.value }))} placeholder="Notes" rows={3} />
        <div style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap" }}>
          <button type="submit" disabled={saving}>{editingId ? "Save Changes" : "Create Entity"}</button>
          {editingId && (
            <button type="button" onClick={() => { setEditingId(null); setForm(emptyForm); }} disabled={saving}>
              Cancel Edit
            </button>
          )}
        </div>
      </form>

      <div style={{ display: "grid", gap: "0.75rem", padding: "1rem", border: "1px solid rgba(255,255,255,0.12)", borderRadius: "12px" }}>
        <strong>Detected Entity Suggestions</strong>
        <p style={{ margin: 0, opacity: 0.8 }}>These are extracted suggestions, not canonical entities yet. Promote one when you want to create a real Canon record.</p>
        {suggestedEntities.length === 0 ? (
          <p style={{ margin: 0 }}>No suggestions waiting.</p>
        ) : (
          suggestedEntities.map((entity) => (
            <div key={entity.id} style={{ padding: "0.75rem", border: "1px dashed rgba(255,255,255,0.18)", borderRadius: "10px" }}>
              <div style={{ display: "flex", justifyContent: "space-between", gap: "1rem", flexWrap: "wrap" }}>
                <div>
                  <strong>{entity.name}</strong> <span style={{ opacity: 0.75 }}>({entity.entityType || entity.type || "concept"})</span>
                  <div style={{ opacity: 0.75, marginTop: "0.25rem" }}>Detected by extraction · Facts: {Array.isArray(entity.facts) ? entity.facts.length : 0}</div>
                </div>
                <button type="button" onClick={() => handlePromoteSuggestion(entity)} disabled={saving}>
                  Create Canon Entity
                </button>
                <button type="button" onClick={() => handleRejectSuggestion(entity)} disabled={saving}>
                  Reject Suggestion
                </button>
              </div>
              {entity.aliases?.length ? <div style={{ marginTop: "0.5rem" }}>Aliases: {entity.aliases.join(", ")}</div> : null}
              {entity.description ? <div style={{ marginTop: "0.25rem" }}>{entity.description}</div> : null}
            </div>
          ))
        )}
      </div>

      <form onSubmit={handleMerge} style={{ display: "grid", gap: "0.75rem", padding: "1rem", border: "1px solid rgba(255,255,255,0.12)", borderRadius: "12px" }}>
        <strong>Merge Entities</strong>
        <div style={{ display: "grid", gap: "0.5rem", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))" }}>
          <select value={mergeSourceId} onChange={(event) => setMergeSourceId(event.target.value)}>
            <option value="">Source entity</option>
            {sortedEntities.map((entity) => (
              <option key={entity.id} value={entity.id}>{entity.name}</option>
            ))}
          </select>
          <select value={mergeTargetId} onChange={(event) => setMergeTargetId(event.target.value)}>
            <option value="">Target entity</option>
            {sortedEntities.map((entity) => (
              <option key={entity.id} value={entity.id}>{entity.name}</option>
            ))}
          </select>
        </div>
        <button type="submit" disabled={saving || !mergeSourceId || !mergeTargetId || mergeSourceId === mergeTargetId}>Merge</button>
      </form>

      <div style={{ display: "grid", gap: "0.75rem" }}>
        {sortedEntities.length === 0 ? (
          <p>No Canon entities found.</p>
        ) : (
          sortedEntities.map((entity) => {
            const isExpanded = expandedEntityId === entity.id || editingId === entity.id;
            const facts = Array.isArray(entity.facts) ? [...entity.facts] : [];
            const sortedFacts = [...facts].sort((a, b) => {
              const aPending = a.status === "pending" ? 0 : 1;
              const bPending = b.status === "pending" ? 0 : 1;
              if (aPending !== bPending) return aPending - bPending;
              return (a.fact || "").localeCompare(b.fact || "");
            });

            return (
            <div key={entity.id} style={{ padding: "1rem", border: "1px solid rgba(255,255,255,0.12)", borderRadius: "12px", background: isExpanded ? "rgba(255,255,255,0.03)" : "transparent" }}>
              <div style={{ display: "flex", justifyContent: "space-between", gap: "1rem", flexWrap: "wrap" }}>
                <div>
                  <strong>{entity.name}</strong> <span style={{ opacity: 0.75 }}>({entity.entityType || entity.type || "concept"})</span>
                  <div style={{ opacity: 0.75, marginTop: "0.25rem" }}>
                    ID: {entity.id} · Version: {entity.version || 1} · Status: {entity.status || "active"} · Sync: {entity.sync_status || "pending"}
                  </div>
                </div>
                <div style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap" }}>
                  <button type="button" onClick={() => setExpandedEntityId((prev) => (prev === entity.id ? null : entity.id))} disabled={saving}>
                    {isExpanded ? "Hide Details" : "View Details"}
                  </button>
                  <button type="button" onClick={() => handleEdit(entity)} disabled={saving}>Edit</button>
                  <button type="button" onClick={() => handleDelete(entity.id)} disabled={saving}>Delete</button>
                </div>
              </div>
              {entity.aliases?.length ? <div style={{ marginTop: "0.5rem" }}>Aliases: {entity.aliases.join(", ")}</div> : null}
              {entity.description ? <div style={{ marginTop: "0.25rem" }}>{entity.description}</div> : null}
              {entity.notes ? <div style={{ marginTop: "0.25rem", opacity: 0.8 }}>{entity.notes}</div> : null}
              <div style={{ marginTop: "0.5rem", opacity: 0.75 }}>
                Facts: {Array.isArray(entity.facts) ? entity.facts.length : 0} · Stories: {entity.story_ids?.length || 0}
              </div>
              {isExpanded ? (
                <div style={{ marginTop: "0.75rem", paddingTop: "0.75rem", borderTop: "1px solid rgba(255,255,255,0.12)" }}>
                  <strong style={{ display: "block", marginBottom: "0.5rem" }}>Facts & Details</strong>
                  {sortedFacts.length === 0 ? (
                    <p style={{ margin: 0, opacity: 0.75 }}>No facts stored for this entity yet.</p>
                  ) : (
                    <div style={{ display: "grid", gap: "0.65rem" }}>
                      {sortedFacts.map((fact) => (
                        <div key={fact.id} style={{ padding: "0.65rem", border: "1px solid rgba(255,255,255,0.08)", borderRadius: "10px" }}>
                          <div style={{ fontWeight: 600 }}>{fact.fact}</div>
                          <div style={{ opacity: 0.8, marginTop: "0.25rem" }}>{formatFactMeta(fact) || "No fact metadata"}</div>
                          <div style={{ marginTop: "0.25rem", opacity: 0.75 }}>
                            Evidence: {fact.evidence?.timeId || "n/a"}
                            {typeof fact.evidence?.start === "number" && typeof fact.evidence?.end === "number" ? ` · span ${fact.evidence.start}-${fact.evidence.end}` : ""}
                          </div>
                          {fact.sourceText ? <div style={{ marginTop: "0.35rem", opacity: 0.75 }}>Source: {fact.sourceText}</div> : null}
                          <div style={{ marginTop: "0.35rem", opacity: 0.7 }}>
                            Match: {typeof fact.entity_match_confidence === "number" ? `${Math.round(fact.entity_match_confidence * 100)}%` : "n/a"}
                            {fact.entity_match_ambiguous ? " · ambiguous" : ""}
                            {Array.isArray(fact.entity_match_candidates) && fact.entity_match_candidates.length > 0 ? ` · candidates: ${fact.entity_match_candidates.length}` : ""}
                          </div>
                          <div style={{ marginTop: "0.6rem", display: "flex", gap: "0.5rem", flexWrap: "wrap" }}>
                            <button
                              type="button"
                              onClick={() => handleFactReview(entity.id, fact, "approved")}
                              disabled={saving}
                            >
                              Accept
                            </button>
                            <button
                              type="button"
                              onClick={() => handleFactReview(entity.id, fact, "rejected")}
                              disabled={saving}
                            >
                              Reject
                            </button>
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              ) : null}
            </div>
            );
          })
        )}
      </div>
    </div>
  );
}

export default CanonDB