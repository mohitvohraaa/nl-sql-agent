import React from 'react';
import './Loading.css';

const STEPS = [
  'Generating SQL...',
  'Validating query...',
  'Running on BigQuery...',
  'Summarizing results...',
];

export default function Loading() {
  const [step, setStep] = React.useState(0);

  React.useEffect(() => {
    const interval = setInterval(() => {
      setStep(s => (s + 1) % STEPS.length);
    }, 2000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="loading">
      <div className="loading-dots">
        <span /><span /><span />
      </div>
      <span className="loading-text">{STEPS[step]}</span>
    </div>
  );
}