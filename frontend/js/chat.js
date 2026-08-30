// Caminho relativo: o próprio FastAPI serve esta página, então a requisição
// vai para a mesma origem e dispensa CORS.
const API_BASE = "/api/chat";

let sessionId = null;

const chatMessages = document.getElementById("chatMessages");
const chatInput = document.getElementById("chatInput");
const btnSend = document.getElementById("btnSend");
const btnClear = document.getElementById("btnClear");

// --- Enviar mensagem ---

// Tempo sem receber nenhum byte do servidor antes de abortar a requisição. O
// contador reinicia a cada pedaço recebido, incluindo os eventos de status e
// os heartbeats do SSE.
const IDLE_TIMEOUT_MS = 45000;

async function sendMessage() {
    const message = chatInput.value.trim();
    if (!message) return;

    chatInput.value = "";
    setInputEnabled(false);

    appendMessage(message, "user");
    showTypingIndicator();

    const controller = new AbortController();
    let idleTimer = null;
    const resetIdleTimer = () => {
        clearTimeout(idleTimer);
        idleTimer = setTimeout(() => controller.abort(), IDLE_TIMEOUT_MS);
    };

    try {
        resetIdleTimer();

        const response = await fetch(`${API_BASE}/stream`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                message: message,
                session_id: sessionId,
            }),
            signal: controller.signal,
        });

        if (!response.ok) {
            throw new Error(`Erro do servidor: ${response.status}`);
        }

        // O indicador de digitação só sai quando o primeiro token chega: os
        // headers do StreamingResponse chegam muito antes disso.
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let botMessage = "";
        let bubbleEl = null;
        let serverError = null;
        let buffer = "";

        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            resetIdleTimer();

            // Um chunk da rede pode cortar uma linha do SSE no meio; o resto
            // fica no buffer para ser completado no próximo chunk.
            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split("\n");
            buffer = lines.pop();

            for (const line of lines) {
                if (!line.startsWith("data: ")) continue;

                let data;
                try {
                    data = JSON.parse(line.slice(6));
                } catch (e) {
                    continue;
                }

                if (data.error) {
                    serverError = data.error;
                }
                // Qual ferramenta está rodando agora, exibido no indicador de
                // digitação e substituído a cada etapa.
                if (data.status) {
                    showTypingIndicator();
                    setTypingStatus(data.status);
                }
                if (data.token) {
                    hideTypingIndicator();
                    if (!bubbleEl) {
                        bubbleEl = appendMessage("", "bot");
                    }
                    botMessage += data.token;
                    bubbleEl.querySelector(".msg-content").innerHTML =
                        marked.parse(botMessage);
                    scrollToBottom();
                }
                if (data.session_id) {
                    sessionId = data.session_id;
                }
            }
        }

        hideTypingIndicator();

        if (serverError) {
            appendMessage(`Erro no servidor: ${serverError}`, "error");
        } else if (!botMessage) {
            appendMessage("Não foi possível gerar uma resposta.", "error");
        }
    } catch (error) {
        hideTypingIndicator();
        if (error.name === "AbortError") {
            appendMessage(
                "A resposta demorou demais e foi cancelada. Isso costuma ser " +
                "limite de uso da API do Google — espere um minuto e tente de novo.",
                "error"
            );
        } else {
            appendMessage(
                "Erro ao se comunicar com o servidor. Verifique se o backend está rodando.",
                "error"
            );
        }
        console.error("Erro no chat:", error);
    } finally {
        clearTimeout(idleTimer);
        setInputEnabled(true);
        chatInput.focus();
    }
}

// --- Limpar sessão ---

async function clearSession() {
    if (sessionId) {
        try {
            await fetch(`${API_BASE}/session/${sessionId}`, {
                method: "DELETE",
            });
        } catch (e) {
            // O histórico local é limpo de qualquer forma.
        }
    }
    sessionId = null;
    chatMessages.innerHTML = "";
    appendMessage(
        "Olá! Sou o assistente do Portal TCC. Posso responder perguntas sobre os dados CSV que foram carregados no sistema. Como posso ajudar?",
        "bot"
    );
    chatInput.focus();
}

// --- Helpers ---

function appendMessage(text, type) {
    const bubble = document.createElement("div");
    bubble.className = `message-bubble ${type}-message`;

    const content = document.createElement("div");
    content.className = "msg-content";

    if (type === "user") {
        content.textContent = text;
    } else {
        content.innerHTML = text ? marked.parse(text) : "";
    }

    bubble.appendChild(content);
    chatMessages.appendChild(bubble);
    scrollToBottom();
    return bubble;
}

function showTypingIndicator() {
    if (document.getElementById("typingIndicator")) return;
    const indicator = document.createElement("div");
    indicator.className = "typing-indicator";
    indicator.id = "typingIndicator";
    indicator.innerHTML =
        '<span></span><span></span><span></span><em class="typing-status"></em>';
    chatMessages.appendChild(indicator);
    scrollToBottom();
}

function setTypingStatus(text) {
    const el = document.querySelector("#typingIndicator .typing-status");
    if (el) {
        el.textContent = text;
        scrollToBottom();
    }
}

function hideTypingIndicator() {
    const indicator = document.getElementById("typingIndicator");
    if (indicator) indicator.remove();
}

function scrollToBottom() {
    chatMessages.scrollTop = chatMessages.scrollHeight;
}

function setInputEnabled(enabled) {
    chatInput.disabled = !enabled;
    btnSend.disabled = !enabled;
}

// --- Event listeners ---

btnSend.addEventListener("click", sendMessage);

chatInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        sendMessage();
    }
});

btnClear.addEventListener("click", clearSession);
