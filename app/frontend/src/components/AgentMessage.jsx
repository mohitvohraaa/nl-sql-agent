import React, { useState } from 'react';
import ResultsTable from './ResultsTable';
import './AgentMessage.css';

export default function AgentMessage({ data }) {
  const [sqlOpen, setSqlOpen] = useState(false);
  const [copied, setCopied]   = useState(false);

  if (data.error) {
    return (
      <div className="agent-message">
        <div className="error-block">⚠ {data.error}</div>
      </div>
    );
  }

  const handleCopy = () => {
    navigator.clipboard.writeText(data.sql || '');
    setCopied(true);
    setTimeout(() => setCopied(false), 1500);
  };

  return (
    <div className="agent-message">

      {/* Summary */}
      {data.summary && (
        <div className="summary-block">
          {data.summary}
        </div>
      )}

      {/* SQL toggle */}
      {data.sql && (
        <div className="sql-block">
          <div className="sql-header" onClick={() => setSqlOpen(o => !o)}>
            <div className="sql-header-left">
              <span className="sql-chevron">{sqlOpen ? '▾' : '▸'}</span>
              <span className="sql-label">GENERATED SQL</span>
            </div>
            <button
              className="copy-btn"
              onClick={(e) => { e.stopPropagation(); handleCopy(); }}
            >
              {copied ? 'COPIED ✓' : 'COPY'}
            </button>
          </div>
          {sqlOpen && <pre className="sql-code">{data.sql}</pre>}
        </div>
      )}

      {/* Results table */}
      {data.rows && data.rows.length > 0 && (
        <ResultsTable columns={data.columns} rows={data.rows} />
      )}

    </div>
  );
}