const gateNoticeTitle = document.body.dataset.noticeTitle;
const gateNoticeMessage = document.body.dataset.noticeMessage;
const accessKey = 'ozur-entry-approved';
const correctDay = '12';
const correctMonth = 'mart';
const defaultRoute = 'ana-sayfa';
const validRoutes = new Set(['ana-sayfa', 'ozur', 'anilar', 'gelecek', 'video']);

let lastNotifiedRoute = '';
let countdownTimer = 0;
let audioUnlockHandlersBound = false;

function getUnlockDeadline() {
    const now = new Date();
    return new Date(now.getFullYear(), now.getMonth(), 11, 23, 30, 0, 0);
}

function isCountdownComplete() {
    return Date.now() >= getUnlockDeadline().getTime();
}

function formatRemainingTime(milliseconds) {
    const totalSeconds = Math.max(0, Math.floor(milliseconds / 1000));
    const days = Math.floor(totalSeconds / 86400);
    const hours = Math.floor((totalSeconds % 86400) / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;

    return `${days}g ${String(hours).padStart(2, '0')}s ${String(minutes).padStart(2, '0')}d ${String(seconds).padStart(2, '0')}sn`;
}

function updateCountdown() {
    const countdownValue = document.querySelector('[data-countdown-value]');
    const countdownNote = document.querySelector('[data-countdown-note]');

    if (!countdownValue || !countdownNote) {
        return;
    }

    const remaining = getUnlockDeadline().getTime() - Date.now();

    if (remaining <= 0) {
        countdownValue.textContent = 'Sayaç bitti.';
        countdownNote.textContent = 'Kilit kalkti. Dogru tarih secildiyse hemen girebilirsin.';

        if (countdownTimer) {
            window.clearInterval(countdownTimer);
            countdownTimer = 0;
        }

        if (hasEntryAccess()) {
            unlockSite();
        }

        return;
    }

    countdownValue.textContent = formatRemainingTime(remaining);
    countdownNote.textContent = 'Bu ayin 11\'i saat 23:30 dolmadan site acilmaz.';
}

function getBackgroundAudio() {
    return document.querySelector('[data-autoplay-audio]');
}

async function playBackgroundAudio() {
    const audio = getBackgroundAudio();

    if (!audio) {
        return false;
    }

    try {
        await audio.play();
        return true;
    } catch {
        // Autoplay can be blocked until the user interacts with the page.
        return false;
    }
}

function bindAudioUnlockHandlers() {
    if (audioUnlockHandlersBound) {
        return;
    }

    const retryPlayback = async () => {
        const didPlay = await playBackgroundAudio();

        if (!didPlay) {
            return;
        }

        audioUnlockHandlersBound = false;
        window.removeEventListener('pointerdown', retryPlayback);
        window.removeEventListener('keydown', retryPlayback);
        window.removeEventListener('touchstart', retryPlayback);
    };

    audioUnlockHandlersBound = true;
    window.addEventListener('pointerdown', retryPlayback, { passive: true });
    window.addEventListener('keydown', retryPlayback);
    window.addEventListener('touchstart', retryPlayback, { passive: true });
}

function syncMediaForRoute(route) {
    const audio = getBackgroundAudio();
    const video = document.querySelector('[data-route-video]');

    if (video && route !== 'video' && !video.paused) {
        video.pause();
    }

    if (!audio) {
        return;
    }

    if (route === 'video') {
        audio.pause();
        return;
    }

    playBackgroundAudio();
}

function createNotice(title, message) {
    if (!title || !message) {
        return;
    }

    let stack = document.querySelector('.notice-stack');

    if (!stack) {
        stack = document.createElement('div');
        stack.className = 'notice-stack';
        document.body.appendChild(stack);
    }

    const notice = document.createElement('section');
    notice.className = 'notice';
    notice.innerHTML = `
        <div class="notice-header">
            <p class="notice-title">${title}</p>
            <button class="notice-close" type="button" aria-label="Bildirimi kapat">×</button>
        </div>
        <p class="notice-text">${message}</p>
    `;

    const closeButton = notice.querySelector('.notice-close');
    const removeNotice = () => {
        notice.classList.remove('is-visible');
        window.setTimeout(() => notice.remove(), 220);
    };

    closeButton.addEventListener('click', removeNotice);
    stack.appendChild(notice);

    window.requestAnimationFrame(() => {
        notice.classList.add('is-visible');
    });

    window.setTimeout(removeNotice, 4800);
}

function hasEntryAccess() {
    return window.localStorage.getItem(accessKey) === 'true';
}

function getCurrentRoute() {
    const route = window.location.hash.replace('#', '').trim();
    return validRoutes.has(route) ? route : defaultRoute;
}

function updateNavState(route) {
    document.querySelectorAll('[data-route-link]').forEach((link) => {
        if (link.dataset.routeLink === route) {
            link.setAttribute('aria-current', 'page');
            return;
        }

        link.removeAttribute('aria-current');
    });
}

function showRouteNotice(route) {
    if (lastNotifiedRoute === route) {
        return;
    }

    const view = document.querySelector(`[data-view="${route}"]`);

    if (!view) {
        return;
    }

    lastNotifiedRoute = route;
    createNotice(view.dataset.noticeTitle, view.dataset.noticeMessage);
}

function renderRoute(options = {}) {
    const { notify = true } = options;
    const route = getCurrentRoute();

    document.querySelectorAll('[data-view]').forEach((view) => {
        view.hidden = view.dataset.view !== route;
    });

    updateNavState(route);
    syncMediaForRoute(route);

    if (notify) {
        window.setTimeout(() => showRouteNotice(route), 220);
    }
}

function setLockedState(isLocked) {
    const gatePanel = document.querySelector('[data-gate-panel]');
    const appShell = document.querySelector('[data-app-shell]');
    const nav = document.querySelector('[data-site-nav]');

    if (gatePanel) {
        gatePanel.hidden = !isLocked;
    }

    if (appShell) {
        appShell.hidden = isLocked;
    }

    if (nav) {
        nav.hidden = isLocked;
    }
}

function unlockSite() {
    if (!isCountdownComplete()) {
        lockSite();
        return;
    }

    setLockedState(false);
    renderRoute({ notify: false });
    window.setTimeout(() => showRouteNotice(getCurrentRoute()), 220);
}

function lockSite() {
    setLockedState(true);
    lastNotifiedRoute = '';
    window.setTimeout(() => createNotice(gateNoticeTitle, gateNoticeMessage), 450);
}

function initEntryForm() {
    const form = document.querySelector('[data-entry-form]');
    const feedback = document.querySelector('[data-entry-feedback]');

    if (!form || !feedback) {
        return;
    }

    form.addEventListener('submit', (event) => {
        event.preventDefault();

        const formData = new FormData(form);
        const day = String(formData.get('day') || '');
        const month = String(formData.get('month') || '');

        if (day === correctDay && month === correctMonth) {
            window.localStorage.setItem(accessKey, 'true');

            if (!isCountdownComplete()) {
                feedback.textContent = 'Secim dogru. Ama sayaç bitmeden site acilmayacak.';
                feedback.classList.add('is-success');
                createNotice('Secim dogru', 'Giris onaylandi. Sayaç bitince site acilacak.');
                updateCountdown();
                return;
            }

            feedback.textContent = 'Secim dogru. Icerik aciliyor.';
            feedback.classList.add('is-success');
            createNotice('Giris basarili', 'Site acildi. Muzik kesilmeden devam edecek.');
            unlockSite();
            return;
        }

        feedback.textContent = 'Bu tarih yanlis. Baska bir gun ve ay kombinasyonu dene.';
        feedback.classList.remove('is-success');
        createNotice('Yanlis tarih', 'Secilen tarih eslesmedi. Tekrar dene.');
    });
}

function initReactionButton() {
    const reactionButtons = Array.from(document.querySelectorAll('[data-reaction]'));
    const reactionOutput = document.querySelector('[data-reaction-output]');

    if (!reactionButtons.length || !reactionOutput) {
        return;
    }

    reactionButtons.forEach((button) => {
        button.addEventListener('click', () => {
            reactionButtons.forEach((item) => item.classList.remove('active', 'is-reacting'));
            reactionOutput.classList.remove('is-updated');
            void button.offsetWidth;
            button.classList.add('active', 'is-reacting');
            reactionOutput.textContent = `Tepki secildi: ${button.dataset.reaction || ''}`;
            reactionOutput.classList.add('is-updated');
            createNotice('Tepki alindi', button.dataset.reaction || 'Tepki secildi.');

            if (typeof telegramLog === 'function') {
                telegramLog(`💬 Giris tepkisi secildi: <b>${button.dataset.reaction || 'Bilinmeyen tepki'}</b>`);
            }
        });
    });
}

function initNavigation() {
    window.addEventListener('hashchange', () => {
        if (!hasEntryAccess()) {
            return;
        }

        renderRoute();
    });

    if (!window.location.hash) {
        window.location.hash = defaultRoute;
    }
}

function initAutoplayAudio() {
    const audio = getBackgroundAudio();

    if (!audio) {
        return;
    }

    audio.load();
    playBackgroundAudio().then((didPlay) => {
        if (!didPlay) {
            bindAudioUnlockHandlers();
        }
    });
}

function initPage() {
    initNavigation();
    initEntryForm();
    initReactionButton();
    initAutoplayAudio();
    updateCountdown();

    if (!countdownTimer && !isCountdownComplete()) {
        countdownTimer = window.setInterval(updateCountdown, 1000);
    }

    if (hasEntryAccess() && isCountdownComplete()) {
        unlockSite();
        return;
    }

    lockSite();
}

if (document.readyState === 'loading') {
    window.addEventListener('DOMContentLoaded', initPage);
} else {
    initPage();
}