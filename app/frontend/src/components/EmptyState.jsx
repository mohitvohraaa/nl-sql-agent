import React from 'react';
import './EmptyState.css';

export default function EmptyState({ suggestions, onSuggestion }) {
  return (
    <div className="empty-state">
      <div className="empty-icon">⬡</div>
      <h2 className="empty-title">// READY</h2>
      <p className="empty-desc">
        Ask a question about the Google Analytics dataset.<br />
        The agent will generate SQL, run it, and summarize the results.
      </p>
      <div className="suggestions">
        {suggestions.map((s, i) => (
          <button key={i} className="suggestion" onClick={() => onSuggestion(s)}>
            {s}
          </button>
        ))}
      </div>
    </div>
  );
}