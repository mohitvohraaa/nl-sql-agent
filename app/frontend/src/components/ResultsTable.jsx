import React, { useState } from 'react';
import './ResultsTable.css';

const PAGE_SIZE = 10;

export default function ResultsTable({ columns, rows }) {
  const [page, setPage] = useState(0);

  const totalPages = Math.ceil(rows.length / PAGE_SIZE);
  const pageRows   = rows.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  return (
    <div className="table-wrap">

      {/* Table meta */}
      <div className="table-meta">
        <span>RESULTS</span>
        <span className="table-count">{rows.length} ROWS · {columns.length} COLS</span>
      </div>

      {/* Table */}
      <div className="table-scroll">
        <table>
          <thead>
            <tr>
              {columns.map(col => (
                <th key={col}>{col}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {pageRows.map((row, i) => (
              <tr key={i}>
                {columns.map(col => (
                  <td key={col}>{formatCell(row[col])}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="pagination">
          <button
            className="page-btn"
            onClick={() => setPage(p => p - 1)}
            disabled={page === 0}
          >
            ← PREV
          </button>
          <span className="page-info">
            {page + 1} / {totalPages}
          </span>
          <button
            className="page-btn"
            onClick={() => setPage(p => p + 1)}
            disabled={page === totalPages - 1}
          >
            NEXT →
          </button>
        </div>
      )}

    </div>
  );
}

function formatCell(value) {
  if (value === null || value === undefined) return '—';
  if (typeof value === 'number') {
    // Format large numbers with commas
    return value.toLocaleString();
  }
  return String(value);
}