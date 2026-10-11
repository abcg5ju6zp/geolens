import { act, render, waitFor } from '@testing-library/react';
import { createMemoryRouter, RouterProvider, type InitialEntry } from 'react-router';
import { useBuilderEntryIntent, type BuilderEntryIntent } from '@/hooks/use-builder-entry-intent';

function renderBuilderAt(initialEntries: InitialEntry[], initialIndex?: number) {
  const intents: BuilderEntryIntent[] = [];
  function Probe() {
    const intent = useBuilderEntryIntent();
    intents.push(intent);
    return null;
  }
  const router = createMemoryRouter(
    [
      { path: '/maps/:id', element: <Probe /> },
      { path: '/datasets/:id', element: null },
    ],
    { initialEntries, initialIndex },
  );
  render(<RouterProvider router={router} />);
  return { router, intents };
}

describe('useBuilderEntryIntent', () => {
  it('ignores an add_dataset query parameter', () => {
    const { intents } = renderBuilderAt(['/maps/map-1?add_dataset=ds-1&chat_result=1']);

    expect(intents.at(-1)).toEqual({});
  });

  it('reads the router state once and clears it so refresh and Back do not repeat it', async () => {
    const { router, intents } = renderBuilderAt(
      [
        '/datasets/ds-1',
        { pathname: '/maps/map-1', search: '?tab=layers', state: { addDatasetId: 'ds-1', chatResult: true } },
      ],
      1,
    );

    expect(intents[0]).toEqual({ addDatasetId: 'ds-1', chatResult: true });
    await waitFor(() => expect(router.state.location.state).toBeNull());
    expect(router.state.location.pathname).toBe('/maps/map-1');
    expect(router.state.location.search).toBe('?tab=layers');
    expect(intents.at(-1)).toEqual({ addDatasetId: 'ds-1', chatResult: true });

    const refreshed = renderBuilderAt([router.state.location]);
    expect(refreshed.intents.at(-1)).toEqual({});

    await act(() => router.navigate('/datasets/ds-1'));
    intents.length = 0;
    await act(() => router.navigate(-1));
    expect(router.state.location.pathname).toBe('/maps/map-1');
    expect(intents.at(-1)).toEqual({});
  });
});
