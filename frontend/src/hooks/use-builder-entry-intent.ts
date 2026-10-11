import { useEffect, useState } from 'react';
import { useLocation, useNavigate } from 'react-router';

/**
 * One-shot work another page asks the map builder to do on arrival. It travels
 * in router state rather than the query string because only this app can set
 * router state; a link from another site can't make the builder save a layer.
 */
export interface BuilderEntryIntent {
  addDatasetId?: string;
  /** A chat query result is waiting in the session handoff (chat-result-handoff.ts). */
  chatResult?: true;
}

function parseIntent(state: unknown): BuilderEntryIntent {
  if (!state || typeof state !== 'object') return {};
  const { addDatasetId, chatResult } = state as Record<string, unknown>;
  return {
    ...(typeof addDatasetId === 'string' && addDatasetId ? { addDatasetId } : {}),
    ...(chatResult === true ? { chatResult: true as const } : {}),
  };
}

/**
 * Returns the intent the builder was opened with, read once at mount, and
 * clears it from the history entry so a refresh or Back doesn't repeat it.
 */
export function useBuilderEntryIntent(): BuilderEntryIntent {
  const location = useLocation();
  const navigate = useNavigate();
  const [intent] = useState(() => parseIntent(location.state));
  const hasIntent = intent.addDatasetId !== undefined || intent.chatResult === true;

  useEffect(() => {
    if (!hasIntent) return;
    navigate(
      { pathname: location.pathname, search: location.search, hash: location.hash },
      { replace: true, state: null },
    );
    // eslint-disable-next-line react-hooks/exhaustive-deps -- clear the entry this page mounted on, once
  }, []);

  return intent;
}
