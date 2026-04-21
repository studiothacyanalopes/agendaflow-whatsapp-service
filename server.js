require("dotenv").config();

const express = require("express");
const cors = require("cors");
const QRCode = require("qrcode");
const fs = require("fs");
const path = require("path");
const { Client, LocalAuth } = require("whatsapp-web.js");
const { createClient } = require("@supabase/supabase-js");

const app = express();
app.use(cors());
app.use(express.json({ limit: "10mb" }));

const PORT = process.env.PORT || 3010;

const SUPABASE_URL =
  process.env.SUPABASE_URL ||
  process.env.NEXT_PUBLIC_SUPABASE_URL ||
  "";

const SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "";

console.log("DEBUG ENV:", {
  PORT: process.env.PORT,
  PUBLIC_APP_URL: process.env.PUBLIC_APP_URL,
  SUPABASE_URL: SUPABASE_URL ? "OK" : "VAZIO",
  NEXT_PUBLIC_SUPABASE_URL: process.env.NEXT_PUBLIC_SUPABASE_URL ? "OK" : "VAZIO",
  SUPABASE_SERVICE_ROLE_KEY: SUPABASE_SERVICE_ROLE_KEY ? "OK" : "VAZIO",
});

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY são obrigatórios.");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

const clients = new Map();
const recentReplies = new Map();

const initializingClients = new Set();

const GREETING_COOLDOWN_MS = 3 * 60 * 1000; // 3 min
const SAME_REPLY_COOLDOWN_MS = 2 * 60 * 1000; // 2 min
const FALLBACK_COOLDOWN_MS = 3 * 60 * 1000; // 3 min

const AVAILABLE_SLOT_INTERVAL_MINUTES = 30;
const AVAILABLE_SLOT_LOOKAHEAD_DAYS = 5;
const MAX_AVAILABLE_SLOTS_IN_REPLY = 6;
const MAX_SERVICES_IN_REPLY = 10;
const MAX_PROFESSIONALS_IN_REPLY = 10;

const WORKER_INTERVAL_MS = 5 * 60 * 1000;
const REACTIVATION_LOOKBACK_DAYS = 120;

const SILENT_MESSAGES = new Set([
  "ok",
  "okk",
  "blz",
  "beleza",
  "entendi",
  "show",
  "valeu",
  "vlw",
  "obg",
  "obrigado",
  "obrigada",
  "perfeito",
  "certo",
  "ata",
  "aha",
  "aham",
  "hmm",
  "hm",
]);

let automationWorkerRunning = false;

function getClientSessionKey(companyId) {
  return `company-${companyId}`;
}

function getClientAuthPath(companyId) {
  const sessionKey = getClientSessionKey(companyId);
  return path.join(".wwebjs_auth", `session-${sessionKey}`);
}

async function clearClientSessionFiles(companyId) {
  try {
    const authPath = getClientAuthPath(companyId);

    if (fs.existsSync(authPath)) {
      fs.rmSync(authPath, { recursive: true, force: true });
      console.log(`Sessão local removida para ${companyId}: ${authPath}`);
    }
  } catch (error) {
    console.error(`Erro ao limpar sessão local de ${companyId}:`, error);
  }
}

function getConversationKey(companyId, phone) {
  return `${companyId}:${phone}`;
}

function normalizeText(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^\w\s]/g, " ")
    .replace(/\s+/g, " ")
    .toLowerCase()
    .trim();
}

function normalizePhone(value) {
  return String(value || "").replace(/\D/g, "");
}

function normalizeWhatsappPhoneForBrazil(value) {
  let phone = normalizePhone(value);

  if (!phone) return "";

  // já tem DDI 55
  if (phone.startsWith("55") && phone.length >= 12) {
    return phone;
  }

  // DDD + celular (11 dígitos) => adiciona 55
  if (phone.length === 11) {
    return `55${phone}`;
  }

  // DDD + fixo (10 dígitos) => adiciona 55
  if (phone.length === 10) {
    return `55${phone}`;
  }

  return phone;
}

function getRecentReplyState(companyId, phone) {
  const key = getConversationKey(companyId, phone);

  if (!recentReplies.has(key)) {
    recentReplies.set(key, {
      lastGreetingAt: 0,
      lastReplyText: "",
      lastReplyAt: 0,
      lastFallbackAt: 0,
      lastSilentAt: 0,
    });
  }

  return recentReplies.get(key);
}

function saveRecentReplyState(companyId, phone, payload) {
  const key = getConversationKey(companyId, phone);
  const prev = getRecentReplyState(companyId, phone);

  recentReplies.set(key, {
    ...prev,
    ...payload,
  });
}

function shouldSkipGreeting(state) {
  return Date.now() - Number(state?.lastGreetingAt || 0) < GREETING_COOLDOWN_MS;
}

function shouldSkipSameReply(state, nextReplyText) {
  if (!nextReplyText) return false;

  const sameReply =
    String(state?.lastReplyText || "").trim() === String(nextReplyText).trim();

  if (!sameReply) return false;

  return Date.now() - Number(state?.lastReplyAt || 0) < SAME_REPLY_COOLDOWN_MS;
}

function shouldSkipFallback(state) {
  return Date.now() - Number(state?.lastFallbackAt || 0) < FALLBACK_COOLDOWN_MS;
}

function splitKeywords(keywordField) {
  return String(keywordField || "")
    .split(",")
    .map((item) => normalizeText(item))
    .filter(Boolean);
}

function isGreeting(text) {
  const greetings = [
    "oi",
    "ola",
    "olá",
    "bom dia",
    "boa tarde",
    "boa noite",
    "e ai",
    "e aí",
    "opa",
    "olaa",
    "hello",
  ];

  return greetings.some((item) => text === normalizeText(item));
}

function isSilentMessage(text) {
  return SILENT_MESSAGES.has(normalizeText(text));
}

function containsAny(text, terms) {
  return terms.some((term) => text.includes(normalizeText(term)));
}

function startOfDay(date) {
  const d = new Date(date);
  d.setHours(0, 0, 0, 0);
  return d;
}

function endOfDay(date) {
  const d = new Date(date);
  d.setHours(23, 59, 59, 999);
  return d;
}

function addDays(date, days) {
  const d = new Date(date);
  d.setDate(d.getDate() + days);
  return d;
}

function setTimeOnDate(date, hhmm) {
  const [hour, minute] = String(hhmm || "08:00")
    .split(":")
    .map((item) => Number(item || 0));

  const d = new Date(date);
  d.setHours(hour, minute, 0, 0);
  return d;
}

const APP_TIMEZONE = process.env.APP_TIMEZONE || "America/Sao_Paulo";

function formatDateBR(value) {
  const date = new Date(value);
  return date.toLocaleDateString("pt-BR", {
    timeZone: APP_TIMEZONE,
  });
}

function formatTimeBR(value) {
  const date = new Date(value);
  return date.toLocaleTimeString("pt-BR", {
    hour: "2-digit",
    minute: "2-digit",
    timeZone: APP_TIMEZONE,
  });
}

function formatDateTimeBR(value) {
  return `${formatDateBR(value)} às ${formatTimeBR(value)}`;
}

function formatDayLabel(date) {
  const today = startOfDay(new Date());
  const tomorrow = addDays(today, 1);
  const target = startOfDay(date);

  if (target.getTime() === today.getTime()) return "hoje";
  if (target.getTime() === tomorrow.getTime()) return "amanhã";

  return target.toLocaleDateString("pt-BR", {
    weekday: "long",
    day: "2-digit",
    month: "2-digit",
  });
}

function buildPublicLink(company) {
  const appUrl = process.env.PUBLIC_APP_URL || "http://localhost:3000";
  return `${appUrl}/agendar/${company.slug}`;
}

function buildCompanyAddress(company) {
  return [
    company?.address_name,
    company?.address_street,
    company?.address_number,
    company?.address_neighborhood,
    company?.address_city,
    company?.address_state,
  ]
    .filter(Boolean)
    .join(", ");
}

function fillTemplate(template, company, extras = {}) {
  const replacements = {
    "{nome_empresa}": company?.name || "Minha empresa",
    "{link_agendamento}": company?.slug ? buildPublicLink(company) : "",
    "{whatsapp_empresa}": company?.brand_whatsapp || "",
    "{maps_link}": company?.maps_link || "",
    "{endereco_empresa}": buildCompanyAddress(company),
    "{hora_inicio_empresa}": company?.business_hours?.start || "08:00",
    "{hora_fim_empresa}": company?.business_hours?.end || "18:00",
    "{nome_cliente}": extras?.["{nome_cliente}"] || "",
    "{telefone_cliente}": extras?.["{telefone_cliente}"] || "",
    "{nome_profissional}": extras?.["{nome_profissional}"] || "",
    "{nome_servico}": extras?.["{nome_servico}"] || "",
    "{data_agendamento}": extras?.["{data_agendamento}"] || "",
    "{hora_inicio}": extras?.["{hora_inicio}"] || "",
    "{valor_agendamento}": extras?.["{valor_agendamento}"] || "",
    ...extras,
  };

  let result = String(template || "");

  for (const [key, value] of Object.entries(replacements)) {
    result = result.split(key).join(String(value || ""));
  }

  return result.trim();
}

function getMinutesDiffFromNow(targetDate) {
  return Math.round((new Date(targetDate).getTime() - Date.now()) / 60000);
}

function overlaps(startA, endA, startB, endB) {
  return startA < endB && endA > startB;
}

function makeMessageHash(text) {
  return normalizeText(text)
    .replace(/\s+/g, "_")
    .slice(0, 140);
}

async function upsertConnection(companyId, payload) {
  const { error } = await supabase
    .from("whatsapp_connections")
    .upsert(
      {
        company_id: companyId,
        ...payload,
      },
      { onConflict: "company_id" }
    );

  if (error) {
    console.error("Erro ao salvar conexão:", error.message);
  }
}

async function insertMessageLog(payload) {
  try {
    await supabase.from("whatsapp_message_logs").insert(payload);
  } catch (error) {
    console.error("Erro ao salvar whatsapp_message_logs:", error?.message || error);
  }
}

async function reserveAutomationDispatch({
  companyId,
  appointmentId,
  messageType,
}) {
  try {
    const { data, error } = await supabase
      .from("whatsapp_automation_dispatches")
      .insert({
        company_id: companyId,
        appointment_id: appointmentId,
        message_type: messageType,
        status: "pending",
      })
      .select("id")
      .single();

    if (error) {
      // duplicate key = já reservado/enviado antes
      if (error.code === "23505") {
        return null;
      }

      console.error("Erro ao reservar dispatch:", error.message);
      return null;
    }

    return data;
  } catch (error) {
    console.error("Erro em reserveAutomationDispatch:", error?.message || error);
    return null;
  }
}

async function markAutomationDispatchSent({
  companyId,
  appointmentId,
  messageType,
}) {
  try {
    const { error } = await supabase
      .from("whatsapp_automation_dispatches")
      .update({
        status: "sent",
        sent_at: new Date().toISOString(),
        last_error: null,
      })
      .eq("company_id", companyId)
      .eq("appointment_id", appointmentId)
      .eq("message_type", messageType);

    if (error) {
      console.error("Erro ao marcar dispatch como sent:", error.message);
    }
  } catch (error) {
    console.error("Erro em markAutomationDispatchSent:", error?.message || error);
  }
}

async function markAutomationDispatchFailed({
  companyId,
  appointmentId,
  messageType,
  errorMessage,
}) {
  try {
    const { error } = await supabase
      .from("whatsapp_automation_dispatches")
      .update({
        status: "failed",
        failed_at: new Date().toISOString(),
        last_error: errorMessage || null,
      })
      .eq("company_id", companyId)
      .eq("appointment_id", appointmentId)
      .eq("message_type", messageType);

    if (error) {
      console.error("Erro ao marcar dispatch como failed:", error.message);
    }
  } catch (error) {
    console.error("Erro em markAutomationDispatchFailed:", error?.message || error);
  }
}

async function wasMessageRecentlySent({
  companyId,
  messageType,
  content,
  lookbackHours = 72,
}) {
  try {
    const startDate = new Date(Date.now() - lookbackHours * 60 * 60 * 1000);

    const { data, error } = await supabase
      .from("whatsapp_message_logs")
      .select("id, status")
      .eq("company_id", companyId)
      .eq("direction", "outbound")
      .eq("message_type", messageType)
      .eq("content", content)
      .in("status", ["queued", "sent"])
      .gte("created_at", startDate.toISOString())
      .limit(1);

    if (error) {
      console.error("Erro ao validar mensagem já enviada:", error.message);
      return false;
    }

    return !!(data && data.length > 0);
  } catch (error) {
    console.error("Erro em wasMessageRecentlySent:", error?.message || error);
    return false;
  }
}

async function wasAutomationAlreadySent({
  companyId,
  appointmentId,
  messageType,
  lookbackHours = 72,
}) {
  try {
    if (!appointmentId) return false;

    const startDate = new Date(Date.now() - lookbackHours * 60 * 60 * 1000);

    const { data, error } = await supabase
      .from("whatsapp_message_logs")
      .select("id, status, metadata")
      .eq("company_id", companyId)
      .eq("direction", "outbound")
      .eq("message_type", messageType)
      .gte("created_at", startDate.toISOString());

    if (error) {
      console.error("Erro ao validar automação já enviada:", error.message);
      return false;
    }

    return (data || []).some((item) => {
      const meta = item?.metadata || {};
      return (
        String(meta.appointment_id || "") === String(appointmentId) &&
        ["queued", "sent", "failed"].includes(String(item.status || ""))
      );
    });
  } catch (error) {
    console.error("Erro em wasAutomationAlreadySent:", error?.message || error);
    return false;
  }
}


async function waitForClientReady(client, timeoutMs = 20000) {
  if (!client) return false;

  if (client.info?.wid?.user) {
    return true;
  }

  return new Promise((resolve) => {
    let finished = false;

    const cleanup = () => {
      client.removeListener("ready", onReady);
      client.removeListener("authenticated", onAuthenticated);
      client.removeListener("auth_failure", onFailure);
      client.removeListener("disconnected", onFailure);
    };

    const done = (result) => {
      if (finished) return;
      finished = true;
      cleanup();
      resolve(result);
    };

    const onReady = () => done(true);
    const onAuthenticated = () => {
      // autenticou, mas ainda vamos aguardar o ready
    };
    const onFailure = () => done(false);

    client.once("ready", onReady);
    client.once("authenticated", onAuthenticated);
    client.once("auth_failure", onFailure);
    client.once("disconnected", onFailure);

    setTimeout(() => done(!!client.info?.wid?.user), timeoutMs);
  });
}

function isClientReallyReady(client) {
  return !!client?.info?.wid?.user;
}

async function getCompanyContext(companyId) {
  const { data, error } = await supabase
    .from("companies")
    .select(`
      id,
      name,
      slug,
      brand_whatsapp,
      maps_link,
      address_name,
      address_street,
      address_number,
      address_neighborhood,
      address_city,
      address_state,
      business_hours
    `)
    .eq("id", companyId)
    .maybeSingle();

  if (error) {
    console.error("Erro ao buscar empresa:", error.message);
    return null;
  }

  return data;
}

async function getBotSettings(companyId) {
  const { data, error } = await supabase
    .from("whatsapp_bot_settings")
    .select("*")
    .eq("company_id", companyId)
    .maybeSingle();

  if (error) {
    console.error("Erro ao buscar whatsapp_bot_settings:", error.message);
    return null;
  }

  return data;
}

async function getKeywordReplies(companyId) {
  const { data, error } = await supabase
    .from("whatsapp_keyword_replies")
    .select("*")
    .eq("company_id", companyId)
    .eq("is_enabled", true)
    .order("sort_order", { ascending: true });

  if (error) {
    console.error("Erro ao buscar palavras-chave:", error.message);
    return [];
  }

  return data || [];
}

async function getMessageTemplates(companyId) {
  const { data, error } = await supabase
    .from("whatsapp_message_templates")
    .select("*")
    .eq("company_id", companyId);

  if (error) {
    console.error("Erro ao buscar whatsapp_message_templates:", error.message);
    return [];
  }

  return data || [];
}

async function getServices(companyId) {
  const { data, error } = await supabase
    .from("service_items")
    .select("id, title")
    .eq("company_id", companyId)
    .order("title", { ascending: true })
    .limit(30);

  if (error) {
    console.error("Erro ao buscar serviços:", error.message);
    return [];
  }

  return data || [];
}

async function getProfessionals(companyId) {
  const { data, error } = await supabase
    .from("professionals")
    .select("id, name")
    .eq("company_id", companyId)
    .order("name", { ascending: true })
    .limit(30);

  if (error) {
    console.error("Erro ao buscar profissionais:", error.message);
    return [];
  }

  return data || [];
}

async function getClients(companyId) {
  const { data, error } = await supabase
    .from("clients")
    .select("id, name, phone")
    .eq("company_id", companyId)
    .not("phone", "is", null);

  if (error) {
    console.error("Erro ao buscar clients:", error.message);
    return [];
  }

  return data || [];
}

async function findClientByPhone(companyId, phone) {
  const incoming = normalizePhone(phone);
  if (!incoming) return null;

  const companyClients = await getClients(companyId);

  return (
    companyClients.find((client) => {
      const clientPhone = normalizePhone(client.phone);
      if (!clientPhone) return false;

      return (
        incoming.endsWith(clientPhone) ||
        clientPhone.endsWith(incoming) ||
        incoming.includes(clientPhone) ||
        clientPhone.includes(incoming)
      );
    }) || null
  );
}

async function getAppointmentsBetween(companyId, fromDate, toDate) {
const { data, error } = await supabase
  .from("appointments")
  .select(`
    id,
    client_id,
    professional_id,
    service_id,
    start_at,
    end_at,
    status,
    amount,
    created_at,
    updated_at
  `)
  .eq("company_id", companyId)
  .gte("start_at", fromDate.toISOString())
  .lte("start_at", toDate.toISOString())
  .order("start_at", { ascending: true });

  if (error) {
    console.error("Erro ao buscar appointments:", error.message);
    return [];
  }

  return data || [];
}

async function hydrateAppointments(companyId, baseRows) {
  if (!baseRows || !baseRows.length) return [];

  const clientIds = [...new Set(baseRows.map((item) => item.client_id).filter(Boolean))];
  const professionalIds = [...new Set(baseRows.map((item) => item.professional_id).filter(Boolean))];
  const serviceIds = [...new Set(baseRows.map((item) => item.service_id).filter(Boolean))];

  const [clientsRes, professionalsRes, servicesRes] = await Promise.all([
    clientIds.length
      ? supabase.from("clients").select("id, name, phone").in("id", clientIds)
      : Promise.resolve({ data: [], error: null }),
    professionalIds.length
      ? supabase.from("professionals").select("id, name").in("id", professionalIds)
      : Promise.resolve({ data: [], error: null }),
    serviceIds.length
      ? supabase.from("service_items").select("id, title").in("id", serviceIds)
      : Promise.resolve({ data: [], error: null }),
  ]);

  const clientsMap = new Map((clientsRes.data || []).map((item) => [item.id, item]));
  const professionalsMap = new Map((professionalsRes.data || []).map((item) => [item.id, item]));
  const servicesMap = new Map((servicesRes.data || []).map((item) => [item.id, item]));

  return baseRows.map((item) => ({
    ...item,
    client_name: clientsMap.get(item.client_id)?.name || "Cliente",
    client_phone: clientsMap.get(item.client_id)?.phone || "",
    professional_name:
      professionalsMap.get(item.professional_id)?.name || "Profissional",
    service_title: servicesMap.get(item.service_id)?.title || "Serviço",
  }));
}

async function getUpcomingAppointmentsPreview(companyId, limit = 5) {
  const now = new Date();
  const end = addDays(now, 30);

  const rows = await getAppointmentsBetween(companyId, now, end);

  if (!rows.length) return [];

  const filtered = rows
    .filter((item) => !["cancelled", "no_show"].includes(String(item.status || "")))
    .slice(0, limit);

  return hydrateAppointments(companyId, filtered);
}

async function getLastCompletedAppointmentForClient(companyId, clientId) {
  if (!clientId) return null;

  const { data, error } = await supabase
    .from("appointments")
    .select(`
      id,
      client_id,
      professional_id,
      service_id,
      start_at,
      end_at,
      status,
      amount
    `)
    .eq("company_id", companyId)
    .eq("client_id", clientId)
    .eq("status", "completed")
    .order("start_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    console.error("Erro ao buscar último atendimento do cliente:", error.message);
    return null;
  }

  if (!data) return null;

  const hydrated = await hydrateAppointments(companyId, [data]);
  return hydrated[0] || null;
}

function detectIntent(text) {
  if (isGreeting(text)) return "greeting";

  if (
    containsAny(text, [
      "endereco",
      "localizacao",
      "onde fica",
      "como chegar",
      "mapa",
      "rua",
      "local",
    ])
  ) {
    return "address";
  }

  if (
    containsAny(text, [
      "profissional",
      "profissionais",
      "quem atende",
      "quem corta",
      "barbeiro",
      "barbeiros",
      "cabeleireiro",
      "com quem",
      "andre",
      "henrique",
    ])
  ) {
    return "professionals";
  }

  if (
    containsAny(text, [
      "servico",
      "servicos",
      "o que voces fazem",
      "quais servicos",
      "atendimento",
      "corte",
      "barba",
      "sobrancelha",
    ])
  ) {
    return "services";
  }

  if (
    containsAny(text, [
      "preco",
      "precos",
      "valor",
      "valores",
      "quanto custa",
      "tabela de preco",
    ])
  ) {
    return "prices";
  }

  if (
    containsAny(text, [
      "pix",
      "pagamento",
      "pagar",
      "cartao",
      "dinheiro",
      "forma de pagamento",
      "como paga",
    ])
  ) {
    return "payment";
  }

  if (
    containsAny(text, [
      "horario",
      "horarios",
      "funcionamento",
      "aberto",
      "fecha",
      "que horas",
    ])
  ) {
    return "hours";
  }

  if (
    containsAny(text, [
      "vago",
      "vagos",
      "disponivel",
      "disponiveis",
      "agenda",
      "agendar",
      "marcar",
      "encaixe",
      "tem horario",
      "tem vaga",
      "horario livre",
      "proximo horario",
    ])
  ) {
    return "availability";
  }

  return null;
}

function extractTargetDateFromText(text) {
  const normalized = normalizeText(text);
  const now = new Date();

  if (normalized.includes("amanha")) return startOfDay(addDays(now, 1));
  if (normalized.includes("hoje")) return startOfDay(now);

  return startOfDay(now);
}

async function getAvailableSlots(companyId, company, text) {
  const baseDate = extractTargetDateFromText(text);
  const startTime = company?.business_hours?.start || "08:00";
  const endTime = company?.business_hours?.end || "18:00";

  const from = startOfDay(baseDate);
  const to = endOfDay(addDays(baseDate, AVAILABLE_SLOT_LOOKAHEAD_DAYS - 1));

  const appointments = await getAppointmentsBetween(companyId, from, to);

  const busyAppointments = appointments.filter(
    (item) => !["cancelled", "no_show"].includes(String(item.status || ""))
  );

  const found = [];

  for (let dayOffset = 0; dayOffset < AVAILABLE_SLOT_LOOKAHEAD_DAYS; dayOffset += 1) {
    const currentDay = addDays(baseDate, dayOffset);
    const windowStart = setTimeOnDate(currentDay, startTime);
    const windowEnd = setTimeOnDate(currentDay, endTime);

    let slot = new Date(windowStart);

    while (slot < windowEnd) {
      const slotEnd = new Date(
        slot.getTime() + AVAILABLE_SLOT_INTERVAL_MINUTES * 60 * 1000
      );

      if (slotEnd > windowEnd) break;

      const isBusy = busyAppointments.some((appointment) => {
        const apptStart = new Date(appointment.start_at);
        const apptEnd = new Date(appointment.end_at);
        return overlaps(slot, slotEnd, apptStart, apptEnd);
      });

      if (!isBusy && slot > new Date()) {
        found.push({
          start: new Date(slot),
          end: new Date(slotEnd),
        });
      }

      if (found.length >= MAX_AVAILABLE_SLOTS_IN_REPLY) {
        return found;
      }

      slot = new Date(
        slot.getTime() + AVAILABLE_SLOT_INTERVAL_MINUTES * 60 * 1000
      );
    }
  }

  return found;
}

function buildAvailabilityReply(company, slots, knownClient) {
  if (!slots.length) {
    return fillTemplate(
      `${
        knownClient?.name ? `Oi, ${knownClient.name}! ` : ""
      }No momento não encontrei horários vagos nos próximos dias por aqui. Você pode agendar direto neste link: {link_agendamento}`,
      company
    );
  }

  const firstDayLabel = formatDayLabel(slots[0].start);

  const lines = slots.map((slot) => {
    return `• ${formatDayLabel(slot.start)} às ${formatTimeBR(slot.start)}`;
  });

  return fillTemplate(
    `${
      knownClient?.name ? `Oi, ${knownClient.name}! ` : ""
    }Encontrei estes próximos horários vagos na {nome_empresa} (${firstDayLabel} em diante):\n${lines.join(
      "\n"
    )}\n\nPara garantir seu horário, acesse: {link_agendamento}`,
    company
  );
}

function buildServicesReply(company, services, knownClient) {
  if (!services.length) {
    return fillTemplate(
      `${
        knownClient?.name ? `Oi, ${knownClient.name}! ` : ""
      }No momento não encontrei a lista de serviços cadastrada. Você pode ver e agendar por aqui: {link_agendamento}`,
      company
    );
  }

  const list = services
    .slice(0, MAX_SERVICES_IN_REPLY)
    .map((item) => `• ${item.title}`)
    .join("\n");

  return fillTemplate(
    `${
      knownClient?.name ? `Oi, ${knownClient.name}! ` : ""
    }Estes são alguns serviços da {nome_empresa}:\n${list}\n\nPara ver tudo e agendar, acesse: {link_agendamento}`,
    company
  );
}

function buildProfessionalsReply(company, professionals, knownClient, text) {
  if (!professionals.length) {
    return fillTemplate(
      `${
        knownClient?.name ? `Oi, ${knownClient.name}! ` : ""
      }No momento não encontrei profissionais cadastrados para exibir. Você pode agendar por aqui: {link_agendamento}`,
      company
    );
  }

  const normalized = normalizeText(text || "");
  const matchingProfessionals = professionals.filter((professional) =>
    normalizeText(professional.name).includes(normalized) ||
    normalized.includes(normalizeText(professional.name))
  );

  const chosenList =
    matchingProfessionals.length > 0 ? matchingProfessionals : professionals;

  const list = chosenList
    .slice(0, MAX_PROFESSIONALS_IN_REPLY)
    .map((item) => `• ${item.name}`)
    .join("\n");

  return fillTemplate(
    `${
      knownClient?.name ? `Oi, ${knownClient.name}! ` : ""
    }Temos estes profissionais atendendo na {nome_empresa}:\n${list}\n\nPara escolher o profissional e agendar, acesse: {link_agendamento}`,
    company
  );
}

function buildUpcomingAppointmentsReply(company, upcomingAppointments, knownClient) {
  if (!upcomingAppointments.length) {
    return "";
  }

  const lines = upcomingAppointments.slice(0, 3).map((item) => {
    return `• ${item.service_title} com ${item.professional_name} em ${formatDateTimeBR(
      item.start_at
    )}`;
  });

  return fillTemplate(
    `${
      knownClient?.name ? `Oi, ${knownClient.name}! ` : ""
    }Próximos horários já agendados na {nome_empresa}:\n${lines.join(
      "\n"
    )}\n\nPara escolher o seu, acesse: {link_agendamento}`,
    company
  );
}

function buildPricesReply(company, settings, knownClient) {
  if (settings?.prices_template?.trim()) {
    return fillTemplate(
      `${knownClient?.name ? `Oi, ${knownClient.name}! ` : ""}${settings.prices_template}`,
      company
    );
  }

  return fillTemplate(
    `${
      knownClient?.name ? `Oi, ${knownClient.name}! ` : ""
    }Para consultar nossos serviços e valores, acesse: {link_agendamento}`,
    company
  );
}

function buildAddressReply(company, settings, knownClient) {
  if (settings?.address_template?.trim()) {
    return fillTemplate(
      `${knownClient?.name ? `Oi, ${knownClient.name}! ` : ""}${settings.address_template}`,
      company
    );
  }

  return fillTemplate(
    `${
      knownClient?.name ? `Oi, ${knownClient.name}! ` : ""
    }Estamos localizados em {endereco_empresa}. Localização: {maps_link}`,
    company
  );
}

function buildPaymentReply(company, settings, knownClient) {
  if (settings?.payment_template?.trim()) {
    return fillTemplate(
      `${knownClient?.name ? `Oi, ${knownClient.name}! ` : ""}${settings.payment_template}`,
      company
    );
  }

  return fillTemplate(
    `${
      knownClient?.name ? `Oi, ${knownClient.name}! ` : ""
    }Aceitamos os meios de pagamento configurados pela empresa. Para mais detalhes ou agendar, acesse: {link_agendamento}`,
    company
  );
}

function buildHoursReply(company, settings, knownClient) {
  if (settings?.hours_template?.trim()) {
    return fillTemplate(
      `${knownClient?.name ? `Oi, ${knownClient.name}! ` : ""}${settings.hours_template}`,
      company
    );
  }

    return fillTemplate(
    `${
      knownClient?.name ? `Oi, ${knownClient.name}! ` : ""
    }Nosso horário de atendimento é de {hora_inicio_empresa} até {hora_fim_empresa}. Para agendar, acesse: {link_agendamento}`,
    company
  );
}

function buildGreetingReply(company, settings, knownClient, lastCompletedAppointment) {
  if (knownClient?.name && lastCompletedAppointment) {
    return fillTemplate(
      `Oi, ${knownClient.name}! Que bom falar com você novamente 👋 Vi que seu último atendimento foi ${lastCompletedAppointment.service_title} com ${lastCompletedAppointment.professional_name}. Quer agendar de novo, ver horários vagos ou falar com a equipe? Link: {link_agendamento}`,
      company
    );
  }

  if (knownClient?.name) {
    return fillTemplate(
      `Oi, ${knownClient.name}! Que bom falar com você novamente 👋 Posso te ajudar com horários, profissionais, serviços e agendamento. Acesse também: {link_agendamento}`,
      company
    );
  }

  const template =
    settings?.greeting_template ||
    "Olá! 👋 Bem-vindo à {nome_empresa}. Para agendar seu horário, acesse: {link_agendamento}";

  return fillTemplate(template, company);
}

function buildFallbackReply(company, settings, knownClient) {
  const template =
    settings?.fallback_template ||
    "Olá! Posso te ajudar com agendamento, horário, endereço e pagamentos. Para agendar agora, acesse: {link_agendamento}";

  if (knownClient?.name) {
    return fillTemplate(`Oi, ${knownClient.name}! ${template}`, company);
  }

  return fillTemplate(template, company);
}

function matchKeywordReply(text, keywordReplies) {
  for (const reply of keywordReplies) {
    if (!reply?.is_enabled) continue;

    const keywords = splitKeywords(reply.keyword);
    if (!keywords.length) continue;

    const matched =
      reply.match_type === "exact"
        ? keywords.some((keyword) => text === keyword)
        : keywords.some((keyword) => text.includes(keyword));

    if (matched) {
      return reply;
    }
  }

  return null;
}

async function buildReplyFromIntent({
  intent,
  text,
  companyId,
  company,
  settings,
  knownClient,
  lastCompletedAppointment,
}) {
  if (intent === "greeting") {
    return {
      replyText: buildGreetingReply(
        company,
        settings,
        knownClient,
        lastCompletedAppointment
      ),
      messageType: "greeting",
    };
  }

  if (intent === "address") {
    return {
      replyText: buildAddressReply(company, settings, knownClient),
      messageType: "address",
    };
  }

  if (intent === "payment") {
    return {
      replyText: buildPaymentReply(company, settings, knownClient),
      messageType: "payment",
    };
  }

  if (intent === "prices") {
    return {
      replyText: buildPricesReply(company, settings, knownClient),
      messageType: "prices",
    };
  }

  if (intent === "hours") {
    return {
      replyText: buildHoursReply(company, settings, knownClient),
      messageType: "hours",
    };
  }

  if (intent === "services") {
    const services = await getServices(companyId);
    return {
      replyText: buildServicesReply(company, services, knownClient),
      messageType: "services",
    };
  }

  if (intent === "professionals") {
    const professionals = await getProfessionals(companyId);
    return {
      replyText: buildProfessionalsReply(
        company,
        professionals,
        knownClient,
        text
      ),
      messageType: "professionals",
    };
  }

  if (intent === "availability") {
    const slots = await getAvailableSlots(companyId, company, text);

    if (slots.length) {
      return {
        replyText: buildAvailabilityReply(company, slots, knownClient),
        messageType: "availability",
      };
    }

    const upcomingAppointments = await getUpcomingAppointmentsPreview(companyId);
    const upcomingText = buildUpcomingAppointmentsReply(
      company,
      upcomingAppointments,
      knownClient
    );

    return {
      replyText:
        upcomingText ||
        fillTemplate(
          `${
            knownClient?.name ? `Oi, ${knownClient.name}! ` : ""
          }Você pode escolher seu melhor horário por aqui: {link_agendamento}`,
          company
        ),
      messageType: "availability",
    };
  }

  return null;
}

async function sendWhatsappMessage(
  companyId,
  phone,
  text,
  messageType = "manual",
  metadata = {}
) {
  const sessionKey = getClientSessionKey(companyId);
  let client = clients.get(sessionKey);

  console.log("DEBUG SEND WHATSAPP:", {
    companyId,
    sessionKey,
    hasClient: !!client,
    phone,
    messageType,
    preview: String(text || "").slice(0, 120),
    metadata,
  });

  const normalizedPhone = normalizeWhatsappPhoneForBrazil(phone);

  if (!normalizedPhone) {
    console.log("Telefone inválido para envio:", phone);
    return false;
  }

  // tenta restaurar sessão se o banco diz conectado mas o client sumiu da memória
  if (!client) {
    try {
      console.log(`Cliente não estava em memória. Tentando restaurar sessão: ${companyId}`);
      client = await createWhatsappClient(companyId);
    } catch (restoreError) {
      console.error("Erro ao restaurar sessão WhatsApp:", restoreError);
    }
  }

  if (!client) {
    console.log(`Cliente WhatsApp não encontrado para ${companyId}`);
    return false;
  }

  const isReady = await waitForClientReady(client);

  if (!isReady) {
    console.log(`Cliente WhatsApp ainda não ficou pronto para ${companyId}`);
    return false;
  }

  try {
    console.log("DEBUG SEND WHATSAPP NORMALIZED PHONE:", normalizedPhone);

    let resolvedWid = null;

    try {
      resolvedWid = await client.getNumberId(normalizedPhone);
      console.log("DEBUG SEND WHATSAPP RESOLVED WID:", resolvedWid);
    } catch (resolveError) {
      console.error("Erro ao resolver getNumberId:", resolveError);
    }

    const finalPhone =
      resolvedWid?._serialized ||
      (String(phone || "").includes("@")
        ? String(phone)
        : `${normalizedPhone}@c.us`);

    if (!finalPhone || finalPhone === "status@broadcast") {
      console.log("Destino inválido para envio:", finalPhone);
      return false;
    }

    console.log("DEBUG SEND WHATSAPP FINAL PHONE:", finalPhone);

    await insertMessageLog({
      company_id: companyId,
      direction: "outbound",
      message_type: messageType,
      phone: finalPhone,
      content: text,
      status: "queued",
      metadata,
    });

    await client.sendMessage(finalPhone, text);

    await insertMessageLog({
      company_id: companyId,
      direction: "outbound",
      message_type: messageType,
      phone: finalPhone,
      content: text,
      status: "sent",
      metadata,
    });

    console.log("DEBUG SEND WHATSAPP SUCCESS:", {
      companyId,
      finalPhone,
      messageType,
      metadata,
    });

    return true;
  } catch (error) {
    console.error("Erro ao enviar mensagem WhatsApp:", error);

    await insertMessageLog({
      company_id: companyId,
      direction: "outbound",
      message_type: messageType,
      phone: normalizedPhone,
      content: text,
      status: "failed",
      metadata: {
        ...metadata,
        error: error?.message || "unknown_error",
      },
    });

    return false;
  }
}

async function processAutomationForCompany(companyId) {
  const [company, settings, templates] = await Promise.all([
    getCompanyContext(companyId),
    getBotSettings(companyId),
    getMessageTemplates(companyId),
  ]);

  console.log("DEBUG WORKER COMPANY:", {
    companyId,
    hasCompany: !!company,
    botEnabled: !!settings?.is_enabled,
    templatesCount: Array.isArray(templates) ? templates.length : 0,
  });

  if (!company || !settings?.is_enabled) return;

  const now = new Date();
  const from = new Date(now.getTime() - 2 * 60 * 60 * 1000);
  const to = addDays(now, 7);

  const baseAppointments = await getAppointmentsBetween(companyId, from, to);
  const appointments = await hydrateAppointments(companyId, baseAppointments);

  for (const appointment of appointments) {
    console.log("DEBUG APPOINTMENT LOOP:", {
      companyId,
      appointment_id: appointment.id,
      status: appointment.status,
      client_name: appointment.client_name,
      client_phone: appointment.client_phone,
      start_at: appointment.start_at,
    });



    if (!appointment.client_phone) {
      console.log("DEBUG APPOINTMENT SEM TELEFONE, IGNORADO:", appointment.id);
      continue;
    }


    const minutesDiff = getMinutesDiffFromNow(appointment.start_at);

        console.log("DEBUG TIME CHECK:", {
  now_iso: new Date().toISOString(),
  start_at_iso: new Date(appointment.start_at).toISOString(),
  now_local: new Date().toString(),
  start_at_local: new Date(appointment.start_at).toString(),
  minutesDiff,
});

    const templateConfirmation = templates.find(
      (item) => item.template_type === "confirmation" && item.is_enabled
    );

    const template24h = templates.find(
      (item) => item.template_type === "reminder_24h" && item.is_enabled
    );

const template1h = templates.find(
  (item) =>
    ["reminder_1h", "lembrete_1h", "1h"].includes(String(item.template_type || "")) &&
    item.is_enabled
);

    const templateCompleted = templates.find(
      (item) => item.template_type === "completed" && item.is_enabled
    );

    const templateCancellation = templates.find(
      (item) => item.template_type === "cancellation" && item.is_enabled
    );

    const templateReschedule = templates.find(
      (item) => item.template_type === "reschedule" && item.is_enabled
    );

    console.log("DEBUG TEMPLATE FLAGS:", {
  companyId,
  templatesCount: templates.length,
  hasConfirmation: !!templateConfirmation,
  hasReminder24h: !!template24h,
  hasReminder1h: !!template1h,
  hasCompleted: !!templateCompleted,
  hasCancellation: !!templateCancellation,
  hasReschedule: !!templateReschedule,
});

    const extras = {
      "{nome_cliente}": appointment.client_name || "Cliente",
      "{telefone_cliente}": appointment.client_phone || "",
      "{nome_profissional}": appointment.professional_name || "Profissional",
      "{nome_servico}": appointment.service_title || "Serviço",
      "{data_agendamento}": formatDateBR(appointment.start_at),
      "{hora_inicio}": formatTimeBR(appointment.start_at),
      "{valor_agendamento}": String(appointment.amount || ""),
    };

const createdAtMs = appointment.created_at
  ? new Date(appointment.created_at).getTime()
  : 0;

const updatedAtMs = appointment.updated_at
  ? new Date(appointment.updated_at).getTime()
  : 0;

const appointmentChangedRecently =
  (createdAtMs && Date.now() - createdAtMs <= 15 * 60 * 1000) ||
  (updatedAtMs && Date.now() - updatedAtMs <= 15 * 60 * 1000);

if (
  templateConfirmation &&
  settings.send_confirmation &&
  ["scheduled", "confirmed"].includes(String(appointment.status || "")) &&
  minutesDiff >= -15 &&
  appointmentChangedRecently
) {

  const content = fillTemplate(
    templateConfirmation.message_template,
    company,
    extras
  );

console.log("DEBUG CONFIRMATION BLOCK:", {
  companyId,
  appointment_id: appointment.id,
  client_id: appointment.client_id || null,
  professional_id: appointment.professional_id || null,
  service_id: appointment.service_id || null,
  status: appointment.status,
  client_name: appointment.client_name,
  client_phone: appointment.client_phone,
  professional_name: appointment.professional_name,
  service_title: appointment.service_title,
  start_at: appointment.start_at,
  created_at: appointment.created_at || null,
  updated_at: appointment.updated_at || null,
  minutesDiff,
  appointmentChangedRecently,
  content,
});

  const dispatch = await reserveAutomationDispatch({
    companyId,
    appointmentId: appointment.id,
    messageType: "confirmation",
  });

  const alreadySent = !dispatch;

  console.log("DEBUG CONFIRMATION ALREADY SENT:", {
    appointment_id: appointment.id,
    alreadySent,
  });

  if (!alreadySent) {
    const sent = await sendWhatsappMessage(
      companyId,
      appointment.client_phone,
      content,
      "confirmation",
      {
        appointment_id: appointment.id,
        template_type: "confirmation",
        start_at: appointment.start_at,
      }
    );

    if (sent) {
      await markAutomationDispatchSent({
        companyId,
        appointmentId: appointment.id,
        messageType: "confirmation",
      });
    } else {
      await markAutomationDispatchFailed({
        companyId,
        appointmentId: appointment.id,
        messageType: "confirmation",
        errorMessage: "sendWhatsappMessage returned false",
      });
    }

    console.log("DEBUG CONFIRMATION RESULT:", {
      appointment_id: appointment.id,
      sent,
    });
  }
}

    if (
      template24h &&
      settings.send_reminder_24h &&
      ["scheduled", "confirmed"].includes(String(appointment.status || "")) &&
      minutesDiff >= 1380 &&
      minutesDiff <= 1500
    ) {
      const content = fillTemplate(
        template24h.message_template,
        company,
        extras
      );

      const dispatch = await reserveAutomationDispatch({
        companyId,
        appointmentId: appointment.id,
        messageType: "reminder_24h",
      });

      const alreadySent = !dispatch;

      if (!alreadySent) {
        const sent = await sendWhatsappMessage(
          companyId,
          appointment.client_phone,
          content,
          "reminder_24h",
          {
            appointment_id: appointment.id,
            template_type: "reminder_24h",
            start_at: appointment.start_at,
          }
        );

        if (sent) {
          await markAutomationDispatchSent({
            companyId,
            appointmentId: appointment.id,
            messageType: "reminder_24h",
          });
        } else {
          await markAutomationDispatchFailed({
            companyId,
            appointmentId: appointment.id,
            messageType: "reminder_24h",
            errorMessage: "sendWhatsappMessage returned false",
          });
        }
      }
    }


    console.log("DEBUG REMINDER WINDOW CHECK:", {
  appointment_id: appointment.id,
  start_at: appointment.start_at,
  minutesDiff,
  status: appointment.status,
  send_reminder_1h: settings.send_reminder_1h,
  has_template1h: !!template1h,
});

    if (
      template1h &&
      settings.send_reminder_1h &&
      ["scheduled", "confirmed"].includes(String(appointment.status || "")) &&
      minutesDiff >= 30 &&
      minutesDiff <= 90
    ) {
      const content = fillTemplate(
        template1h.message_template,
        company,
        extras
      );

      const dispatch = await reserveAutomationDispatch({
        companyId,
        appointmentId: appointment.id,
        messageType: "reminder_1h",
      });

      const alreadySent = !dispatch;

      console.log("DEBUG REMINDER 1H:", {
        appointment_id: appointment.id,
        minutesDiff,
        alreadySent,
        start_at: appointment.start_at,
      });

      if (!alreadySent) {
        const sent = await sendWhatsappMessage(
          companyId,
          appointment.client_phone,
          content,
          "reminder_1h",
          {
            appointment_id: appointment.id,
            template_type: "reminder_1h",
            start_at: appointment.start_at,
          }
        );

        if (sent) {
          await markAutomationDispatchSent({
            companyId,
            appointmentId: appointment.id,
            messageType: "reminder_1h",
          });
        } else {
          await markAutomationDispatchFailed({
            companyId,
            appointmentId: appointment.id,
            messageType: "reminder_1h",
            errorMessage: "sendWhatsappMessage returned false",
          });
        }
      }
    }

    if (
      templateCompleted &&
      appointment.status === "completed"
    ) {
      const content = fillTemplate(
        templateCompleted.message_template,
        company,
        extras
      );

      const dispatch = await reserveAutomationDispatch({
        companyId,
        appointmentId: appointment.id,
        messageType: "completed",
      });

      const alreadySent = !dispatch;

      if (!alreadySent) {
        const sent = await sendWhatsappMessage(
          companyId,
          appointment.client_phone,
          content,
          "completed",
          {
            appointment_id: appointment.id,
            template_type: "completed",
            start_at: appointment.start_at,
          }
        );

        if (sent) {
          await markAutomationDispatchSent({
            companyId,
            appointmentId: appointment.id,
            messageType: "completed",
          });
        } else {
          await markAutomationDispatchFailed({
            companyId,
            appointmentId: appointment.id,
            messageType: "completed",
            errorMessage: "sendWhatsappMessage returned false",
          });
        }
      }
    }

    if (
      templateCancellation &&
      settings.send_cancellation &&
      appointment.status === "cancelled"
    ) {
      const content = fillTemplate(
        templateCancellation.message_template,
        company,
        extras
      );

      const dispatch = await reserveAutomationDispatch({
        companyId,
        appointmentId: appointment.id,
        messageType: "cancellation",
      });

      const alreadySent = !dispatch;

      if (!alreadySent) {
        const sent = await sendWhatsappMessage(
          companyId,
          appointment.client_phone,
          content,
          "cancellation",
          {
            appointment_id: appointment.id,
            template_type: "cancellation",
            start_at: appointment.start_at,
          }
        );

        if (sent) {
          await markAutomationDispatchSent({
            companyId,
            appointmentId: appointment.id,
            messageType: "cancellation",
          });
        } else {
          await markAutomationDispatchFailed({
            companyId,
            appointmentId: appointment.id,
            messageType: "cancellation",
            errorMessage: "sendWhatsappMessage returned false",
          });
        }
      }
    }

    if (
      templateReschedule &&
      settings.send_reschedule &&
      appointment.status === "rescheduled"
    ) {
      const content = fillTemplate(
        templateReschedule.message_template,
        company,
        extras
      );

      const dispatch = await reserveAutomationDispatch({
        companyId,
        appointmentId: appointment.id,
        messageType: "reschedule",
      });

      const alreadySent = !dispatch;

      if (!alreadySent) {
        const sent = await sendWhatsappMessage(
          companyId,
          appointment.client_phone,
          content,
          "reschedule",
          {
            appointment_id: appointment.id,
            template_type: "reschedule",
            start_at: appointment.start_at,
          }
        );

        if (sent) {
          await markAutomationDispatchSent({
            companyId,
            appointmentId: appointment.id,
            messageType: "reschedule",
          });
        } else {
          await markAutomationDispatchFailed({
            companyId,
            appointmentId: appointment.id,
            messageType: "reschedule",
            errorMessage: "sendWhatsappMessage returned false",
          });
        }
      }
    }
  }

  const templateReactivation = templates.find(
    (item) => item.template_type === "reactivation" && item.is_enabled
  );

  if (templateReactivation && settings.send_reactivation) {
    const clientsList = await getClients(companyId);

    for (const clientRow of clientsList) {
      if (!clientRow.phone) continue;

      const lastCompleted = await getLastCompletedAppointmentForClient(
        companyId,
        clientRow.id
      );

      if (!lastCompleted) continue;

      const daysWithoutReturn = Math.floor(
        (Date.now() - new Date(lastCompleted.start_at).getTime()) /
          (1000 * 60 * 60 * 24)
      );

      const triggerDays =
        Number(templateReactivation.trigger_days_after || 0) ||
        Number(settings.reactivation_days || 15);

      if (
        daysWithoutReturn < triggerDays ||
        daysWithoutReturn > REACTIVATION_LOOKBACK_DAYS
      ) {
        continue;
      }

      const content = fillTemplate(
        templateReactivation.message_template,
        company,
        {
          "{nome_cliente}": clientRow.name || "Cliente",
          "{telefone_cliente}": clientRow.phone || "",
          "{nome_profissional}": lastCompleted.professional_name || "",
          "{nome_servico}": lastCompleted.service_title || "",
          "{data_agendamento}": formatDateBR(lastCompleted.start_at),
          "{hora_inicio}": formatTimeBR(lastCompleted.start_at),
          "{valor_agendamento}": String(lastCompleted.amount || ""),
        }
      );

      const alreadySent = await wasMessageRecentlySent({
        companyId,
        messageType: "reactivation",
        content,
      });

      if (!alreadySent) {
        await sendWhatsappMessage(
          companyId,
          clientRow.phone,
          content,
          "reactivation"
        );
      }
    }
  }
}

async function runAutomationWorker() {
  if (automationWorkerRunning) return;

  automationWorkerRunning = true;

  try {
    const { data: connections, error } = await supabase
      .from("whatsapp_connections")
      .select("company_id, status")
      .eq("status", "connected");

    if (error) {
      console.error("Erro ao buscar conexões para worker:", error.message);
      return;
    }

    for (const connection of connections || []) {
      try {
        await processAutomationForCompany(connection.company_id);
      } catch (error) {
        console.error(
          `Erro no worker da empresa ${connection.company_id}:`,
          error
        );
      }
    }
  } catch (error) {
    console.error("Erro geral no worker:", error);
  } finally {
    automationWorkerRunning = false;
  }
}

async function setupIncomingMessageHandler(client, companyId) {
  client.on("message", async (message) => {
    try {
      const from = String(message.from || "").trim();

      if (!from) return;
      if (message.fromMe) return;
      if (from.includes("@g.us")) return;
      if (from === "status@broadcast") return;
      if (from.includes("@broadcast")) return;
      if (from.includes("@newsletter")) return;
      if (from.includes("@call")) return;
      if (from.includes("@lid")) return;

      const originalText = String(message.body || "").trim();
      const text = normalizeText(originalText);
      if (!text) return;

      console.log("Mensagem recebida:", text);

      await insertMessageLog({
        company_id: companyId,
        direction: "inbound",
        message_type: "incoming",
        phone: message.from,
        content: originalText,
        status: "received",
      });

      const [company, settings, keywordReplies, knownClient] = await Promise.all([
        getCompanyContext(companyId),
        getBotSettings(companyId),
        getKeywordReplies(companyId),
        findClientByPhone(companyId, message.from),
      ]);

      if (!company || !settings?.is_enabled || !settings?.auto_reply_enabled) {
        return;
      }

      if (isSilentMessage(text)) {
        console.log("Mensagem silenciosa ignorada:", message.from);
        saveRecentReplyState(companyId, message.from, {
          lastSilentAt: Date.now(),
        });
        return;
      }

      const lastCompletedAppointment = knownClient?.id
        ? await getLastCompletedAppointmentForClient(companyId, knownClient.id)
        : null;

      const state = getRecentReplyState(companyId, message.from);
      const greeting = isGreeting(text);
      const detectedIntent = detectIntent(text);

      let replyText = "";
      let messageType = "fallback";

      if (greeting) {
        if (shouldSkipGreeting(state)) {
          console.log("Saudação ignorada por cooldown:", message.from);
          return;
        }

        replyText = buildGreetingReply(
          company,
          settings,
          knownClient,
          lastCompletedAppointment
        );
        messageType = "greeting";
      } else {
        const keywordMatch = matchKeywordReply(text, keywordReplies);

        if (detectedIntent) {
          const intentReply = await buildReplyFromIntent({
            intent: detectedIntent,
            text,
            companyId,
            company,
            settings,
            knownClient,
            lastCompletedAppointment,
          });

          if (intentReply?.replyText) {
            replyText = intentReply.replyText;
            messageType = intentReply.messageType;
          }
        }

        if (!replyText && keywordMatch?.reply_template) {
          replyText = fillTemplate(
            `${knownClient?.name ? `Oi, ${knownClient.name}! ` : ""}${
              keywordMatch.reply_template
            }`,
            company
          );
          messageType = "keyword_reply";
        }

        if (!replyText) {
          if (shouldSkipFallback(state)) {
            console.log("Fallback ignorado por cooldown:", message.from);
            return;
          }

          replyText = buildFallbackReply(company, settings, knownClient);
          messageType = "fallback";
        }
      }

      if (!replyText.trim()) return;

      if (shouldSkipSameReply(state, replyText)) {
        console.log("Resposta repetida ignorada:", message.from);
        return;
      }

      await client.sendMessage(message.from, replyText);

      const nextState = {
        lastReplyText: replyText,
        lastReplyAt: Date.now(),
      };

      if (messageType === "greeting") {
        nextState.lastGreetingAt = Date.now();
      }

      if (messageType === "fallback") {
        nextState.lastFallbackAt = Date.now();
      }

      saveRecentReplyState(companyId, message.from, nextState);

      await insertMessageLog({
        company_id: companyId,
        direction: "outbound",
        message_type: messageType,
        phone: message.from,
        content: replyText,
        status: "sent",
      });
    } catch (error) {
      console.error("Erro ao responder mensagem recebida:", error);
    }
  });
}


async function createWhatsappClient(companyId, options = {}) {
  const sessionKey = getClientSessionKey(companyId);
  const forceReset = options?.forceReset === true;

  const existingClient = clients.get(sessionKey);

    const pairWithPhoneNumber = options?.pairWithPhoneNumber?.phoneNumber
    ? {
        phoneNumber: normalizeWhatsappPhoneForBrazil(
          options.pairWithPhoneNumber.phoneNumber
        ),
        showNotification:
          options?.pairWithPhoneNumber?.showNotification !== false,
        intervalMs: Number(options?.pairWithPhoneNumber?.intervalMs || 180000),
      }
    : null;

  if (existingClient && isClientReallyReady(existingClient) && !forceReset) {
    console.log(`Cliente já pronto em memória para ${companyId}`);
    return existingClient;
  }

  if (initializingClients.has(sessionKey) && !forceReset) {
    console.log(`Sessão já está inicializando: ${companyId}`);
    return clients.get(sessionKey) || null;
  }

  if (forceReset) {
    try {
      if (existingClient) {
        await existingClient.destroy().catch(() => null);
      }
    } catch (error) {
      console.error(`Erro ao destruir client antigo de ${companyId}:`, error);
    }

    clients.delete(sessionKey);
    initializingClients.delete(sessionKey);

    await clearClientSessionFiles(companyId);

    await upsertConnection(companyId, {
      status: "disconnected",
      phone: null,
      qr_code: null,
      last_disconnected_at: new Date().toISOString(),
    });
  }

  initializingClients.add(sessionKey);

  const client = new Client({
    authStrategy: new LocalAuth({
      clientId: sessionKey,
      dataPath: ".wwebjs_auth",
    }),
    puppeteer: {
      headless: true,
      args: ["--no-sandbox", "--disable-setuid-sandbox"],
    },
    ...(pairWithPhoneNumber
      ? {
          pairWithPhoneNumber,
        }
      : {}),
  });

  client.on("qr", async (qr) => {
    try {
      const qrCodeDataUrl = await QRCode.toDataURL(qr);

      await upsertConnection(companyId, {
        status: "disconnected",
        phone: null,
        qr_code: qrCodeDataUrl,
        last_connected_at: null,
        last_disconnected_at: null,
      });

      console.log(`QR gerado para ${companyId}`);
    } catch (error) {
      console.error(`Erro ao gerar QR para ${companyId}:`, error);
    }
  });

  client.on("authenticated", async () => {
    console.log(`Autenticado: ${companyId}`);
  });

  client.on("ready", async () => {
    const info = client.info;
    const wid = info?.wid?.user || null;

    await upsertConnection(companyId, {
      status: "connected",
      phone: wid,
      qr_code: null,
      last_connected_at: new Date().toISOString(),
      last_disconnected_at: null,
    });

    console.log(`Cliente pronto: ${companyId}`);
  });

  client.on("auth_failure", async (message) => {
    console.error(`Falha de autenticação ${companyId}:`, message);

    await upsertConnection(companyId, {
      status: "disconnected",
      phone: null,
      qr_code: null,
      last_disconnected_at: new Date().toISOString(),
    });

    clients.delete(sessionKey);
    initializingClients.delete(sessionKey);
  });

  client.on("disconnected", async (reason) => {
    await upsertConnection(companyId, {
      status: "disconnected",
      phone: null,
      qr_code: null,
      last_disconnected_at: new Date().toISOString(),
    });

    clients.delete(sessionKey);
    initializingClients.delete(sessionKey);
    console.log(`Cliente desconectado: ${companyId}`, reason || "");
  });

  await setupIncomingMessageHandler(client, companyId);

  clients.set(sessionKey, client);

  try {
    console.log("INICIANDO CLIENTE WHATSAPP:", {
      companyId,
      sessionKey,
      forceReset,
    });

    await client.initialize();

    console.log("CLIENTE INITIALIZE CHAMADO COM SUCESSO:", {
      companyId,
      sessionKey,
      forceReset,
    });

    return client;
  } catch (error) {
    clients.delete(sessionKey);
    initializingClients.delete(sessionKey);

    const message = error?.message || "";

    if (message.includes("The browser is already running")) {
      console.error(
        `Sessão já está em uso por outro processo para ${companyId}.`
      );
      return null;
    }

    console.error(`Erro ao inicializar cliente WhatsApp ${companyId}:`, error);
    return null;
  } finally {
    initializingClients.delete(sessionKey);
  }
}


async function restoreConnectedSessions() {
  try {
    const { data, error } = await supabase
      .from("whatsapp_connections")
      .select("company_id, status")
      .eq("status", "connected");

    if (error) {
      console.error("Erro ao restaurar sessões conectadas:", error.message);
      return;
    }

    for (const row of data || []) {
      try {
        console.log("Restaurando sessão WhatsApp:", row.company_id);

        const restoredClient = await createWhatsappClient(row.company_id);

        if (!restoredClient) {
          console.log(
            `Sessão ${row.company_id} não foi restaurada agora. Serviço segue rodando sem cair.`
          );
        }
      } catch (err) {
        console.error(`Erro ao restaurar sessão ${row.company_id}:`, err);
      }
    }
  } catch (error) {
    console.error("Erro geral ao restaurar sessões:", error);
  }
}

async function requestPhonePairingCode(companyId, phone) {
  const normalizedPhone = normalizeWhatsappPhoneForBrazil(phone);

  if (!phone) {
    throw new Error("Telefone é obrigatório para gerar o código.");
  }

  if (!normalizedPhone) {
    throw new Error("Telefone inválido.");
  }

  const client = await createWhatsappClient(companyId, {
    forceReset: true,
    pairWithPhoneNumber: {
      phoneNumber: normalizedPhone,
      showNotification: true,
      intervalMs: 180000,
    },
  });

  if (!client) {
    throw new Error("Não foi possível iniciar a sessão do WhatsApp.");
  }

  if (typeof client.requestPairingCode !== "function") {
    throw new Error(
      "A versão atual do whatsapp-web.js não suporta conexão por número."
    );
  }

  const ready = await waitForClientReady(client, 3000);

  if (ready && isClientReallyReady(client)) {
    return {
      connected: true,
      pairingCode: null,
      phone: normalizedPhone,
    };
  }

  const pairingCode = await client.requestPairingCode(normalizedPhone);

  await upsertConnection(companyId, {
    status: "connecting",
    phone: normalizedPhone,
    qr_code: null,
    last_disconnected_at: null,
  });

  return {
    connected: false,
    pairingCode,
    phone: normalizedPhone,
  };
}


app.post("/connect-phone/:companyId", async (req, res) => {
  try {
    const { companyId } = req.params;
    const { phone } = req.body || {};

    console.log("CONNECT PHONE REQUEST RECEBIDO:", { companyId, phone });

    const result = await requestPhonePairingCode(companyId, phone);

    return res.json({
      success: true,
      connected: result.connected,
      pairingCode: result.pairingCode,
      phone: result.phone,
      message: result.connected
        ? "Sessão já conectada."
        : "Código de conexão gerado com sucesso.",
    });
  } catch (error) {
    console.error("ERRO /connect-phone:", error);

    return res.status(500).json({
      success: false,
      message:
        error?.message || "Erro ao gerar código de conexão por telefone.",
    });
  }
});

app.post("/connect/:companyId", async (req, res) => {
  try {
    const { companyId } = req.params;
    const sessionKey = getClientSessionKey(companyId);
    const existingClient = clients.get(sessionKey);

    console.log("CONNECT REQUEST RECEBIDO:", { companyId });

    if (existingClient && isClientReallyReady(existingClient)) {
      console.log("CONNECT REQUEST RESULTADO:", {
        companyId,
        hasClient: true,
        alreadyReady: true,
      });

      return res.json({
        success: true,
        connected: true,
        message: "Sessão já conectada.",
      });
    }

    if (initializingClients.has(sessionKey)) {
      console.log("CONNECT REQUEST RESULTADO:", {
        companyId,
        hasClient: true,
        initializing: true,
      });

      return res.json({
        success: true,
        initializing: true,
        message: "Sessão já está inicializando. Aguarde o QR ou autenticação.",
      });
    }

    const client = await createWhatsappClient(companyId);

    console.log("CONNECT REQUEST RESULTADO:", {
      companyId,
      hasClient: !!client,
      ready: isClientReallyReady(client),
    });

    return res.json({
      success: !!client,
      connected: isClientReallyReady(client),
      message: client
        ? "Inicialização da sessão iniciada."
        : "Não foi possível iniciar a sessão agora.",
    });
  } catch (error) {
    console.error("ERRO /connect:", error);

    return res.status(500).json({
      success: false,
      message: "Erro ao iniciar sessão do WhatsApp.",
    });
  }
});

app.post("/disconnect/:companyId", async (req, res) => {
  try {
    const { companyId } = req.params;
    const sessionKey = getClientSessionKey(companyId);
    const client = clients.get(sessionKey);

    if (client) {
      await client.destroy().catch(() => null);
    }

    clients.delete(sessionKey);
    initializingClients.delete(sessionKey);

    await upsertConnection(companyId, {
      status: "disconnected",
      phone: null,
      qr_code: null,
      last_disconnected_at: new Date().toISOString(),
    });

    return res.json({
      success: true,
      message: "Sessão desconectada.",
    });
  } catch (error) {
    console.error(error);
    return res.status(500).json({
      success: false,
      message: "Erro ao desconectar sessão.",
    });
  }
});

app.post("/reset/:companyId", async (req, res) => {
  try {
    const { companyId } = req.params;
    const sessionKey = getClientSessionKey(companyId);
    const existingClient = clients.get(sessionKey);

    if (existingClient) {
      await existingClient.destroy().catch(() => null);
    }

    clients.delete(sessionKey);
    initializingClients.delete(sessionKey);

    await clearClientSessionFiles(companyId);

    await upsertConnection(companyId, {
      status: "disconnected",
      phone: null,
      qr_code: null,
      last_disconnected_at: new Date().toISOString(),
    });

    const freshClient = await createWhatsappClient(companyId, {
      forceReset: false,
    });

    return res.json({
      success: !!freshClient,
      message: freshClient
        ? "Sessão resetada e reiniciada com sucesso."
        : "Sessão resetada, mas não foi possível iniciar o cliente agora.",
    });
  } catch (error) {
    console.error("ERRO /reset:", error);

    return res.status(500).json({
      success: false,
      message: "Erro ao resetar sessão do WhatsApp.",
    });
  }
});

app.get("/health", async (_req, res) => {
  return res.json({ ok: true });
});

app.post("/worker/run", async (_req, res) => {
  try {
    await runAutomationWorker();

    return res.json({
      success: true,
      message: "Worker executado com sucesso.",
    });
  } catch (error) {
    console.error("Erro ao executar worker manual:", error);

    return res.status(500).json({
      success: false,
      message: "Erro ao executar worker.",
    });
  }
});

setInterval(() => {
  runAutomationWorker().catch((error) => {
    console.error("Erro no intervalo do worker:", error);
  });
}, WORKER_INTERVAL_MS);

app.listen(PORT, async () => {
  console.log(`WhatsApp service rodando na porta ${PORT}`);

  await restoreConnectedSessions();

  runAutomationWorker().catch((error) => {
    console.error("Erro na inicialização do worker:", error);
  });
});