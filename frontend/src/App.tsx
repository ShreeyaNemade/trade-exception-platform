import { Route, Routes } from 'react-router-dom';
import AppLayout, { NAV_ITEMS } from './layouts/AppLayout';
import PlaceholderPage from './components/PlaceholderPage';

export default function App() {
  return (
    <Routes>
      <Route element={<AppLayout />}>
        {NAV_ITEMS.map((item) => (
          <Route
            key={item.path}
            path={item.path}
            element={<PlaceholderPage title={item.label} description={item.description} />}
          />
        ))}
      </Route>
    </Routes>
  );
}
