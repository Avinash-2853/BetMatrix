import { useNavigate, useLocation } from "react-router-dom";
import { Button } from "@/components/ui/button";

const Header = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const isHome = location.pathname === "/";

  return (
    <header className="bg-card border-b border-border px-4 sm:px-6 py-4">
      <div className="flex items-center justify-between max-w-7xl mx-auto">
        <div className="flex items-center gap-2 sm:gap-4">
          <img
            src="/logo.png"
            alt="NFL Logo"
            className="w-12 h-12 sm:w-16 sm:h-16 rounded-full object-cover border border-border bg-card cursor-pointer"
            onClick={() => navigate("/")}
          />
          <h1 className="text-base sm:text-xl font-bold text-foreground tracking-wide cursor-pointer" onClick={() => navigate("/")}>
            NFL MATCH PREDICTIONS
          </h1>
        </div>
        <nav className="hidden md:flex items-center gap-4">
          <Button
            variant={location.pathname === "/" ? "default" : "ghost"}
            onClick={() => navigate("/")}
          >
            Home
          </Button>
          <Button
            variant={location.pathname.startsWith("/predictions") || location.pathname.startsWith("/match") ? "default" : "ghost"}
            onClick={() => navigate("/predictions")}
          >
            Predictions
          </Button>
        </nav>
      </div>
    </header>
  );
};

export default Header;
