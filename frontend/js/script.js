const uploadArea = document.getElementById('uploadArea');
const csvFileInput = document.getElementById('csvFile');
const btnSelect = document.getElementById('btnSelect');
const btnUpload = document.getElementById('btnUpload');
const fileInfo = document.getElementById('fileInfo');
const fileName = document.getElementById('fileName');
const fileSize = document.getElementById('fileSize');
const message = document.getElementById('message');

let selectedFile = null;

btnSelect.addEventListener('click', () => {
    csvFileInput.click();
});

uploadArea.addEventListener('click', (e) => {
    if (e.target !== btnSelect) {
        csvFileInput.click();
    }
});

csvFileInput.addEventListener('change', (e) => {
    handleFileSelect(e.target.files[0]);
});

// --- Drag and drop ---

['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
    uploadArea.addEventListener(eventName, preventDefaults, false);
    document.body.addEventListener(eventName, preventDefaults, false);
});

function preventDefaults(e) {
    e.preventDefault();
    e.stopPropagation();
}

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

uploadArea.addEventListener('drop', (e) => {
    const files = e.dataTransfer.files;
    handleFileSelect(files[0]);
}, false);

// --- Seleção de arquivo ---

function handleFileSelect(file) {
    hideMessage();

    if (!file) {
        return;
    }

    if (!file.name.toLowerCase().endsWith('.csv')) {
        showMessage('Erro: Por favor, selecione um arquivo CSV válido.', 'error');
        resetFileSelection();
        return;
    }

    // Mesmo limite de MAX_FILE_SIZE_MB no backend; a validação aqui só evita
    // um upload que seria rejeitado.
    const maxSize = 10 * 1024 * 1024;
    if (file.size > maxSize) {
        showMessage('Erro: O arquivo não pode exceder 10MB.', 'error');
        resetFileSelection();
        return;
    }

    selectedFile = file;
    displayFileInfo(file);
    btnUpload.disabled = false;
    showMessage('Arquivo selecionado com sucesso!', 'info');
}

function displayFileInfo(file) {
    fileName.textContent = file.name;
    fileSize.textContent = formatFileSize(file.size);
    fileInfo.style.display = 'block';
}

function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';

    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));

    return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i];
}

function resetFileSelection() {
    selectedFile = null;
    csvFileInput.value = '';
    fileInfo.style.display = 'none';
    btnUpload.disabled = true;
}

function showMessage(text, type) {
    message.textContent = text;
    message.className = 'message ' + type;
    message.style.display = 'block';
}

function hideMessage() {
    message.style.display = 'none';
    message.className = 'message';
}

// --- Envio ---

btnUpload.addEventListener('click', async () => {
    if (!selectedFile) {
        showMessage('Erro: Nenhum arquivo selecionado.', 'error');
        return;
    }

    btnUpload.disabled = true;
    btnUpload.textContent = 'Enviando...';

    try {
        const formData = new FormData();
        formData.append('csvFile', selectedFile);

        // Mesma origem: o FastAPI serve esta página.
        const response = await fetch('/api/upload', {
            method: 'POST',
            body: formData
        });

        if (!response.ok) {
            throw new Error(`Erro HTTP: ${response.status}`);
        }

        const result = await response.json();

        showMessage('Arquivo enviado e processado com sucesso!', 'success');
        resetFileSelection();

    } catch (error) {
        console.error('Erro ao enviar arquivo:', error);

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
