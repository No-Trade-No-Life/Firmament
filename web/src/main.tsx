import { StrictMode } from "react"
import { createRoot } from "react-dom/client"
import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { AuthMiniProvider } from "auth-mini-react-components"
import { HashRouter } from "react-router-dom"

import App from "./App"
import "./index.css"

const queryClient = new QueryClient()

createRoot(document.getElementById("root")!).render(
  <StrictMode><QueryClientProvider client={queryClient}><AuthMiniProvider authMiniBaseUrl="https://auth.ntnl.io" audiences={["firma.ntnl.io", "linkit.ntnl.io"]} autoRedirectToLogin><HashRouter><App /></HashRouter></AuthMiniProvider></QueryClientProvider></StrictMode>,
)
