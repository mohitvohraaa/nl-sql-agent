import React, { useState, useRef } from 'react';
import './InputBar.css';

export default function InputBar({ onSend, loading }) {
  const [value, setValue] = useState('');
  const inputRef = useRef(null);

  const handleSend = () => {
    if (!value.trim() || loading) return;
    onSend(value.trim());
    setValue('');
  };

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  return (
    <div className="input-bar">
      <input
        ref={inputRef}
        className="input-field"
        type="text"
        placeholder="Ask a question about your GA4 data..."
        value={value}
        onChange={e => setValue(e.target.value)}
        onKeyDown={handleKeyDown}
        disabled={loading}
        autoFocus
      />
      <button
        className={`send-btn ${loading ? 'loading' : ''}`}
        onClick={handleSend}
        disabled={loading || !value.trim()}
      >
        {loading ? '...' : 'RUN →'}
      </button>
    </div>
  );
}