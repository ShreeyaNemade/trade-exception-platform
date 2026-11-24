import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import App from './App';

describe('App', () => {
  it('renders navigation and the active placeholder page', () => {
    render(
      <MemoryRouter initialEntries={['/exceptions']}>
        <App />
      </MemoryRouter>,
    );
    expect(screen.getByRole('navigation', { name: 'Primary' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'Exceptions' })).toBeInTheDocument();
  });
});
