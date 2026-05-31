module.exports = async (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "8469271411:AAENoiYGwRTwa5wvSE1oeqELsA0y9a8gXCw";
  const CHAT_ID   = process.env.TELEGRAM_CHAT_ID   || "7141351945";

  const { event } = req.body || {};
  const now = new Date().toLocaleString("tr-TR", { timeZone: "Europe/Istanbul" });
  const referer = req.headers["referer"] || req.headers["x-referer"] || "Direkt link";

  let source = referer;
  if (/whatsapp/i.test(referer))   source = "WhatsApp'tan açtı";
  else if (/instagram/i.test(referer)) source = "Instagram'dan açtı";
  else if (/t\.me/i.test(referer)) source = "Telegram'dan açtı";

  let msg = "";
  if (event === "page_open") {
    msg = "💌 Video sayfası açıldı!\n\n📅 " + now + "\n🔗 Kaynak: " + source;
  } else if (event === "button_click") {
    msg = "🔔 \"Son notu oku\" butonuna tıkladı!\n\n📅 " + now;
  } else {
    msg = "📢 Olay: " + (event || "bilinmiyor") + "\n\n📅 " + now;
  }

  try {
    const tgRes = await fetch(
      "https://api.telegram.org/bot" + BOT_TOKEN + "/sendMessage",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ chat_id: CHAT_ID, text: msg }),
      }
    );
    const data = await tgRes.json();
    return res.status(200).json(data);
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
};
