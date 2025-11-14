"""
Streamlit front‑end for the RAG system in `test_better.py`.

Usage:
    streamlit run ui_streamlit.py

This file does NOT modify any logic in `test_better.py`. It only imports it
and provides a simple chat UI, plus a sidebar with ingestion + cost stats.
"""

import os
import time
import traceback
import streamlit as st

# ---- Import your existing backend without altering it ----
from rag_DO import (
        chat,                 # function(message: str, history) -> str
        summary,              # ingestion summary dict
        USAGE_TOTALS,         # defaultdict with pricing/usage totals
        SESSION_ID,           # active session id string
    )


# ------------------------- Page config -------------------------
st.set_page_config(
    page_title="EMMETT.ai",
    page_icon="💬",
    layout="wide",
)

# ------------------------- Simple hardcoded login -------------------------
if "authed" not in st.session_state:
    st.session_state.authed = False
if "user" not in st.session_state:
    st.session_state.user = None

def require_login():
    if st.session_state.authed:
        return
    st.image("LogoAI2.png", width=300)
    st.markdown("### 🔒 Please log in to continue")
    with st.form("login-form", clear_on_submit=False):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submit = st.form_submit_button("Login")

    # ✅ Hardcoded credentials
    if submit:
        if username == "Emmett" and password == "DB55":
            st.session_state.authed = True
            st.session_state.user = username
            st.success("Login successful ✅")
            st.rerun()
        else:
            st.error("Invalid username or password ❌")

require_login()
if not st.session_state.authed:
    st.stop()  # prevent rest of UI from rendering until logged in

# ------------------------- Sidebar ----------------------------
with st.sidebar:
    st.image("LogoAI2.png", width=200)
    st.header("⚙️ Settings")
    st.caption("This UI wraps the existing LangChain + Chroma backend.")


    st.subheader("Session")
    st.text_input("Session ID (from backend)", value=str(SESSION_ID), disabled=True)


    st.divider()


    st.subheader("📦 Ingestion summary")
    try:
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Added/Updated", len(summary.get("changed", [])))
            st.metric("Deleted", len(summary.get("deleted", [])))
        with col2:
            st.metric("Unchanged", len(summary.get("skipped", [])))
            st.metric("Chunks in DB", summary.get("total_in_db", 0))


        u = summary.get("embedding_usage", {})
        st.caption(
        f"Embedding tokens: {u.get('total_tokens', 0)} | Cost: ${u.get('total_cost', 0.0):.6f}"
        )
    except Exception:
        st.info("Ingestion summary not available.")


    st.divider()


    st.subheader("💳 Usage (chat)")
    try:
        st.metric("Prompt tokens", int(USAGE_TOTALS.get("prompt_tokens", 0)))
        st.metric("Completion tokens", int(USAGE_TOTALS.get("completion_tokens", 0)))
        st.metric("Total tokens", int(USAGE_TOTALS.get("total_tokens", 0)))
        st.metric("Total cost", f"${float(USAGE_TOTALS.get('total_cost', 0.0)):.4f}")
        st.caption("Totals reflect this Python process; they reset on restart.")
    except Exception:
        st.info("Usage totals not available.")

# ------------------------- Main area ---------------------------
#st.title("EMMETT.ai")
st.image("LogoAI2.png", width=300)

# Initialize chat history in Streamlit state (UI only). The backend tracks its own history via SESSION_ID.
if "messages" not in st.session_state:
    st.session_state.messages = []  # each item: {"role": "user"|"assistant", "content": str}

# Render existing messages
for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        st.markdown(m["content"]) 

# Chat input (bottom)
user_input = st.chat_input("Type your question…")

if user_input:
    # Show user message immediately
    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.markdown(user_input)

    # Reserve an assistant message container for the response
    with st.chat_message("assistant"):
        placeholder = st.empty()
        try:
            # The backend expects (message, history). It doesn't use the UI history structurally,
            # but we'll pass the messages list for compatibility.
            start = time.time()
            response_text = chat(user_input, st.session_state.messages)
            elapsed = time.time() - start

            # Display the full response when ready (streaming happens inside the LLM backend).
            placeholder.markdown(response_text,unsafe_allow_html=False)
            st.caption(f"Responded in {elapsed:.2f}s")
        except Exception:
            placeholder.error("Something went wrong while calling the backend.")
            st.exception(traceback.format_exc())
            response_text = ""

    # Save assistant reply to UI history
    if response_text:
        st.session_state.messages.append({"role": "assistant", "content": response_text})
