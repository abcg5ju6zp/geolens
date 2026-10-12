import { render, screen } from '@testing-library/react';
import { MemoryRouter, Route, Routes, useLocation } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { useAuthStore } from '@/stores/auth-store';
import { LoginPage } from '../LoginPage';

vi.mock('@/api/auth', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/auth')>()),
  getAuthConfig: vi.fn(() => new Promise(() => {})),
}));

function Landing() {
  return <div data-testid="landing">{useLocation().pathname}</div>;
}

function renderSignedIn(from: string) {
  return render(
    <QueryClientProvider client={new QueryClient()}>
      <MemoryRouter initialEntries={[{ pathname: '/login', state: { from } }]}>
        <Routes>
          <Route path="/login" element={<LoginPage />} />
          <Route path="*" element={<Landing />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe('LoginPage when already signed in', () => {
  beforeEach(() => useAuthStore.setState({ token: 'token' }));
  afterEach(() => useAuthStore.setState({ token: null }));

  it('returns to the in-app page that sent the visitor to sign in', () => {
    renderSignedIn('/maps/map-1');

    expect(screen.getByTestId('landing')).toHaveTextContent('/maps/map-1');
  });

  it.each(['//other.example/x', '/\\other.example/x'])('sends %s to the catalog root', (from) => {
    renderSignedIn(from);

    expect(screen.getByTestId('landing').textContent).toBe('/');
  });
});
