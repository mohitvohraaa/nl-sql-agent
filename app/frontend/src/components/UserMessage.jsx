import React from 'react';
import './UserMessage.css';

export default function UserMessage({ text }) {
  return (
    <div className="user-message">
      <div className="user-bubble">{text}</div>
    </div>
  );
}