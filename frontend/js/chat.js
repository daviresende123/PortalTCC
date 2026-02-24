const API_BASE = "http://localhost:8000/api/chat";

let sessionId = null;

const chatMessages = document.getElementById("chatMessages");
const chatInput = document.getElementById("chatInput");
const btnSend = document.getElementById("btnSend");
const btnClear = document.getElementById("btnClear");

// --- Enviar mensagem ---

async function sendMessage() {
    const message = chatInput.value.trim();
    if (!message) return;

    chatInput.value = "";
    setInputEnabled(false);

    appendMessage(message, "user");
    showTypingIndicator();

    try {
        const response = await fetch(`${API_BASE}/stream`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                message: message,
                session_id: sessionId,
            }),
        });

        if (!response.ok) {
            throw new Error(`Erro do servidor: ${response.status}`);
        }

        hideTypingIndicator();
        const bubbleEl = appendMessage("", "bot");
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let botMessage = "";

        while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            const text = decoder.decode(value, { stream: true });
            const lines = text.split("\n");

            for (const line of lines) {
                if (line.startsWith("data: ")) {
                    try {
                        const data = JSON.parse(line.slice(6));
                        if (data.token) {
                            botMessage += data.token;
                            bubbleEl.querySelector("p").textContent = botMessage;
                            scrollToBottom();
                        }
                        if (data.session_id) {
                            sessionId = data.session_id;
                        }
                    } catch (e) {
                        // Ignora linhas mal-formadas
                    }
                }
                if (line.startsWith("event: done")) {
                    // Próxima linha data terá o session_id
                }
                if (line.startsWith("event: error")) {
                    // Próxima linha data terá o erro
                }
            }
        }

        if (!botMessage) {
            bubbleEl.querySelector("p").textContent =
                "Não foi possível gerar uma resposta.";
        }
    } catch (error) {
        hideTypingIndicator();
        appendMessage(
            "Erro ao se comunicar com o servidor. Verifique se o backend está rodando.",
            "error"
        );
        console.error("Erro no chat:", error);
    } finally {
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
            // Ignora erro ao limpar sessão remota
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

    const p = document.createElement("p");
    p.textContent = text;
    bubble.appendChild(p);

    chatMessages.appendChild(bubble);
    scrollToBottom();
    return bubble;
}

function showTypingIndicator() {
    const indicator = document.createElement("div");
    indicator.className = "typing-indicator";
    indicator.id = "typingIndicator";
    indicator.innerHTML = "<span></span><span></span><span></span>";
    chatMessages.appendChild(indicator);
    scrollToBottom();
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
