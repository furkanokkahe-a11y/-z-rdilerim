// ════════════════════════════════════════════════════════════
//  TELEGRAM BİLDİRİM AYARLARI
// ════════════════════════════════════════════════════════════
var TELEGRAM_BOT_TOKEN = "8469271411:AAENoiYGwRTwa5wvSE1oeqELsA0y9a8gXCw";
var TELEGRAM_CHAT_ID   = "7141351945";
// ════════════════════════════════════════════════════════════

var _sayfaGirisZamani = Date.now();

function _getCihazBilgisi() {
  var ua = navigator.userAgent;
  var cihaz = "💻 Bilgisayar";
  if (/iPhone/i.test(ua))       cihaz = "📱 iPhone";
  else if (/iPad/i.test(ua))    cihaz = "📱 iPad";
  else if (/Android/i.test(ua)) cihaz = "📱 Android";

  var tarayici = "Bilinmiyor";
  if (/Chrome\/[0-9]/i.test(ua) && !/Edg/i.test(ua))        tarayici = "Chrome";
  else if (/Safari\/[0-9]/i.test(ua) && !/Chrome/i.test(ua)) tarayici = "Safari";
  else if (/Firefox/i.test(ua)) tarayici = "Firefox";
  else if (/Edg/i.test(ua))     tarayici = "Edge";

  return cihaz + " · " + tarayici;
}

function _getSaat() {
  var now = new Date();
  var gun  = now.toLocaleDateString("tr-TR", { day: "numeric", month: "long" });
  var saat = now.toLocaleTimeString("tr-TR", { hour: "2-digit", minute: "2-digit" });
  return "🕐 " + gun + " " + saat;
}

function _getInternet() {
  var conn = navigator.connection || navigator.mozConnection || navigator.webkitConnection;
  if (!conn) return null;
  if (conn.type === "wifi") return "📶 WiFi";
  if (conn.type === "cellular") return "📡 Mobil data";
  if (conn.effectiveType) return "📡 " + conn.effectiveType.toUpperCase();
  return null;
}

function _getPil() {
  if (!navigator.getBattery) return Promise.resolve(null);
  return navigator.getBattery().then(function (b) {
    var seviye = Math.round(b.level * 100);
    var durum  = b.charging ? "⚡ şarjda" : "";
    return "🔋 " + seviye + "%" + (durum ? " " + durum : "");
  }).catch(function () { return null; });
}

function _getZiyaret() {
  var key   = "site-ziyaret-sayisi";
  var sayi  = parseInt(localStorage.getItem(key) || "0") + 1;
  localStorage.setItem(key, String(sayi));
  if (sayi === 1) return "🔁 İlk ziyaret";
  return "🔁 " + sayi + ". ziyaret";
}

function _getReferrer() {
  var ref = document.referrer;
  if (!ref) return "📍 Direkt açtı";
  if (/whatsapp/i.test(ref))   return "📍 WhatsApp'tan geldi";
  if (/instagram/i.test(ref))  return "📍 Instagram'dan geldi";
  if (/t\.me/i.test(ref))      return "📍 Telegram'dan geldi";
  return "📍 " + ref.split("/")[2];
}

function telegramLog(mesaj) {
  if (!TELEGRAM_BOT_TOKEN || TELEGRAM_BOT_TOKEN === "BOT_TOKEN_BURAYA") return;

  var satirlar = [
    mesaj,
    "",
    _getCihazBilgisi(),
    _getSaat(),
    _getZiyaret(),
    _getReferrer()
  ];

  var internet = _getInternet();
  if (internet) satirlar.push(internet);

  _getPil().then(function (pil) {
    if (pil) satirlar.push(pil);
    fetch("https://api.telegram.org/bot" + TELEGRAM_BOT_TOKEN + "/sendMessage", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        chat_id: TELEGRAM_CHAT_ID,
        text: "💌 Site Bildirimi\n\n" + satirlar.join("\n"),
        parse_mode: "HTML"
      })
    }).catch(function () {});
  });
}

// Her sayfada kaç saniye kalındığını sayfa kapanırken gönder
(function () {
  var sayfa = window.location.pathname.split("/").pop() || "index.html";
  var isimler = {
    "hosgeldin.html":      "Hoş Geldin",
    "tanisma_girisi.html": "Giriş",
    "anilarimiz.html":     "Fotoğraflar",
    "video_sayfasi.html":  "Video",
    "kapanis.html":        "Son Söz",
    "cevap.html":          "Soru"
  };
  if (!isimler[sayfa]) return;
  window.addEventListener("beforeunload", function () {
    var sure = Math.round((Date.now() - _sayfaGirisZamani) / 1000);
    // sendBeacon ile sayfa kapanırken gönder
    var body = JSON.stringify({
      chat_id: TELEGRAM_CHAT_ID,
      text: "⏱️ \"" + isimler[sayfa] + "\" sayfasında " + sure + " saniye geçirdi.",
      parse_mode: "HTML"
    });
    navigator.sendBeacon
      ? navigator.sendBeacon("https://api.telegram.org/bot" + TELEGRAM_BOT_TOKEN + "/sendMessage",
          new Blob([body], { type: "application/json" }))
      : fetch("https://api.telegram.org/bot" + TELEGRAM_BOT_TOKEN + "/sendMessage", {
          method: "POST", headers: { "Content-Type": "application/json" }, body: body, keepalive: true
        }).catch(function(){});
  });
}());

// Sayfa ziyaret logu — otomatik tetiklenir
(function () {
  var sayfa = window.location.pathname.split("/").pop() || "index.html";
  var mesajlar = {
    "hosgeldin.html":      "🌷 Hoş geldin sayfasını açtı",
    "tanisma_girisi.html": "🌸 Biri siteyi açtı! (Henüz şifreyi girmedi)",
    "anilarimiz.html":     "📸 Fotoğraf sayfasını görüntülüyor",
    "video_sayfasi.html":  "🎬 Video sayfasına geçti",
    "kapanis.html":        "💌 Son mektubu okuyor",
    "cevap.html":          "❓ Soru sayfasına geçti"
  };
  if (mesajlar[sayfa]) telegramLog(mesajlar[sayfa]);
}());
