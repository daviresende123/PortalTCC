// Elementos do DOM
const uploadArea = document.getElementById('uploadArea');
const csvFileInput = document.getElementById('csvFile');
const btnSelect = document.getElementById('btnSelect');
const btnUpload = document.getElementById('btnUpload');
const fileInfo = document.getElementById('fileInfo');
const fileName = document.getElementById('fileName');
const fileSize = document.getElementById('fileSize');
const message = document.getElementById('message');

let selectedFile = null;

// Evento: Botão "Selecionar Arquivo"
btnSelect.addEventListener('click', () => {
    csvFileInput.click();
});

// Evento: Área de upload clicável
uploadArea.addEventListener('click', (e) => {
    if (e.target !== btnSelect) {
        csvFileInput.click();
    }
});

// Evento: Arquivo selecionado via input
csvFileInput.addEventListener('change', (e) => {
    handleFileSelect(e.target.files[0]);
});

// Evento: Prevenir comportamento padrão do drag and drop
['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
    uploadArea.addEventListener(eventName, preventDefaults, false);
    document.body.addEventListener(eventName, preventDefaults, false);
});

function preventDefaults(e) {
    e.preventDefault();
    e.stopPropagation();
}

// Evento: Destacar área quando arrastar arquivo
['dragenter', 'dragover'].forEach(eventName => {
    uploadArea.addEventListener(eventName, () => {
        uploadArea.classList.add('dragover');
    }, false);
});

['dragleave', 'drop'].forEach(eventName => {
    uploadArea.addEventListener(eventName, () => {
        uploadArea.classList.remove('dragover');
    }, false);
});

// Evento: Soltar arquivo
uploadArea.addEventListener('drop', (e) => {
    const files = e.dataTransfer.files;
    handleFileSelect(files[0]);
}, false);

// Função: Processar arquivo selecionado
function handleFileSelect(file) {
    // Limpar mensagens anteriores
    hideMessage();

    if (!file) {
        return;
    }

    // Validar se é arquivo CSV (case-insensitive)
    if (!file.name.toLowerCase().endsWith('.csv')) {
        showMessage('Erro: Por favor, selecione um arquivo CSV válido.', 'error');
        resetFileSelection();
        return;
    }

    // Validar tamanho do arquivo (máximo 10MB)
    const maxSize = 10 * 1024 * 1024; // 10MB em bytes
    if (file.size > maxSize) {
        showMessage('Erro: O arquivo não pode exceder 10MB.', 'error');
        resetFileSelection();
        return;
    }

    // Arquivo válido
    selectedFile = file;
    displayFileInfo(file);
    btnUpload.disabled = false;
    showMessage('Arquivo selecionado com sucesso!', 'info');
}

// Função: Exibir informações do arquivo
function displayFileInfo(file) {
    fileName.textContent = file.name;
    fileSize.textContent = formatFileSize(file.size);
    fileInfo.style.display = 'block';
}

// Função: Formatar tamanho do arquivo
function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';

    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));

    return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i];
}

// Função: Resetar seleção de arquivo
function resetFileSelection() {
    selectedFile = null;
    csvFileInput.value = '';
    fileInfo.style.display = 'none';
    btnUpload.disabled = true;
}

// Função: Mostrar mensagem
function showMessage(text, type) {
    message.textContent = text;
    message.className = 'message ' + type;
    message.style.display = 'block';
}

// Função: Esconder mensagem
function hideMessage() {
    message.style.display = 'none';
    message.className = 'message';
}

// Evento: Botão "Enviar para Banco de Dados"
btnUpload.addEventListener('click', async () => {
    if (!selectedFile) {
        showMessage('Erro: Nenhum arquivo selecionado.', 'error');
        return;
    }

    // Desabilitar botão durante upload
    btnUpload.disabled = true;
    btnUpload.textContent = 'Enviando...';

    try {
        // Criar FormData para enviar arquivo
        const formData = new FormData();
        formData.append('csvFile', selectedFile);

        // Enviar para o backend (AJUSTAR URL quando backend estiver pronto)
        const response = await fetch('/api/upload', {
            method: 'POST',
            body: formData
        });

        if (!response.ok) {
            throw new Error(`Erro HTTP: ${response.status}`);
        }

        const result = await response.json();

        // Sucesso
        showMessage('Arquivo enviado e processado com sucesso!', 'success');
        resetFileSelection();

    } catch (error) {
        console.error('Erro ao enviar arquivo:', error);

        // Verificar se é erro de conexão (backend não existe ainda)
        if (error.message.includes('Failed to fetch') || error.message.includes('NetworkError')) {
            showMessage('Erro: Backend não está disponível. Configure o servidor primeiro.', 'error');
        } else {
            showMessage(`Erro ao enviar arquivo: ${error.message}`, 'error');
        }

        btnUpload.disabled = false;
    } finally {
        btnUpload.textContent = 'Enviar para Banco de Dados';
    }
});
