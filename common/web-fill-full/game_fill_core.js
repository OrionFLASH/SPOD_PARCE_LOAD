/* game_fill_core.js — константы, состояние, раскладки, утилиты */
"use strict";
/* === Встроенный каталог (править здесь или через dual-edit HTML) === */
/* каталог подключается из catalog.js → window.PARAM_REVIEW_CATALOG */
var EMBEDDED_CATALOG = (typeof window !== "undefined" && window.PARAM_REVIEW_CATALOG) ? window.PARAM_REVIEW_CATALOG : null;

var BLOCK = "PROM";
var STANDS_KNOWN = ["PROM", "PSI", "IFT"];
var STANDS_UI = ["PROM", "PSI"];
var DEFAULT_STAND = "PROM";
var CSV_EXPORT_STAND_PROM = new Set(["PROM"]);
var projectBlock = "PROM";
/** Чипы фильтра стенда: наличие PROM/PSI и «только один» */
var STAND_FILTER_PROM_ONLY = "PROM_ONLY";
var STAND_FILTER_PSI_ONLY = "PSI_ONLY";
var STAND_FILTER_CODES = ["PROM", "PSI", STAND_FILTER_PROM_ONLY, STAND_FILTER_PSI_ONLY];
var contestListStandFilter = new Set(["PROM"]);
var LS_PROJECT = "spod_web_fill_full_project_v2";
var CATALOG_URL = "./catalog.json";
var CONTEST_CSV_COLS = ["CONTEST_CODE", "FULL_NAME", "CREATE_DT", "CLOSE_DT", "BUSINESS_STATUS", "CONTEST_TYPE", "CONTEST_DESCRIPTION", "CONTEST_FEATURE", "SHOW_INDICATOR", "PRODUCT_GROUP", "PRODUCT", "CONTEST_SUBJECT", "FACTOR_MARK_TYPE", "CONTEST_INDICATOR_METHOD", "CONTEST_FACTOR_METHOD", "PLAN_METHOD_CODE", "PLAN_MOD_METOD", "PLAN_MOD_VALUE", "FACTOR_MATCH", "CONTEST_PERIOD", "TARGET_TYPE", "SOURCE_UPD_FREQUENCY", "CALC_TYPE", "BUSINESS_BLOCK", "FACT_POST_PROCESSING"];
var REWARD_CSV_COLS = ["REWARD_CODE", "REWARD_TYPE", "FULL_NAME", "REWARD_DESCRIPTION", "REWARD_CONDITION", "REWARD_COST", "REWARD_ADD_DATA"];
var LINK_COLS = ["CONTEST_CODE", "GROUP_CODE", "REWARD_CODE"];
var GROUP_COLS = ["CONTEST_CODE", "GROUP_CODE", "GROUP_VALUE", "GET_CALC_METHOD", "GET_CALC_CRITERION", "ADD_CALC_CRITERION", "ADD_CALC_CRITERION_2", "BASE_CALC_CODE"];
var IND_COLS = ["CONTEST_CODE", "INDICATOR_CALC_TYPE", "INDICATOR_ADD_CALC_TYPE", "FULL_NAME", "INDICATOR_CODE", "INDICATOR_AGG_FUNCTION", "INDICATOR_WEIGHT", "INDICATOR_OBJECT", "INDICATOR_MARK_TYPE", "INDICATOR_MATCH", "INDICATOR_VALUE", "CONTEST_CRITERION", "INDICATOR_FILTER", "CONTESTANT_SELECTION", "CALC_TYPE", "N"];
var SCH_COLS = ["TOURNAMENT_CODE", "PERIOD_TYPE", "START_DT", "END_DT", "RESULT_DT", "PLAN_PERIOD_START_DT", "PLAN_PERIOD_END_DT", "CRITERION_MARK_TYPE", "CRITERION_MARK_VALUE", "FILTER_PERIOD_ARR", "TOURNAMENT_STATUS", "CONTEST_CODE", "TARGET_TYPE", "CALC_TYPE", "TRN_INDICATOR_FILTER"];
var DATE_INFINITE = "4000-01-01";
var MONTHS = ["Январь","Февраль","Март","Апрель","Май","Июнь","Июль","Август","Сентябрь","Октябрь","Ноябрь","Декабрь"];
var DOW = ["пн","вт","ср","чт","пт","сб","вс"];

var catalog=EMBEDDED_CATALOG;
var activeSection="CONTEST";
var activeBadge=0;
var activeSchedule=0;
var activeIndicator=0;
var activeGroup=0;
var activeLink=0;
var activeContest=0;
/** Индекс пары ITEM, выбранной в списке слева; null = смотрим конкурс целиком */
var activePairFocus=null;
/** Фильтр списка конкурсов в сайдбаре */
var contestListQuery="";
/** Режим поиска: starts | contains | equals */
var contestListSearchMode="contains";
/** Показывать турнирные / индивидуальные (награды) в списке и в поиске */
var contestListShowTournament=true;
var contestListShowReward=true;
/** Показывать блок архива внизу списка (по умолчанию выкл.) */
var contestListShowArchive=false;
/** Фильтр среды: оба вкл. = не режем */
var contestListShowProm=true;
var contestListShowTest=true;
/** Сентинел фильтра: поле пустое или массива/ключа нет */
var FILTER_EMPTY="__EMPTY__";
/** Бизнес-блок не из четырёх каталожных кодов */
var FILTER_BB_OTHER="__OTHER__";
var BUSINESS_BLOCK_NAMED=["KMMMB","KMKKSB","AKMKKSB","CSM"];
/** Статусы расписания, по умолчанию «живые» + пустое расписание */
var contestListStatuses=new Set(["АКТИВНЫЙ","ПОДВЕДЕНИЕ ИТОГОВ","ЗАВЕРШЕН",FILTER_EMPTY]);
/** Типы наград REWARD_TYPE (по умолчанию все из каталога) */
var REWARD_TYPE_FILTER_FALLBACK=["BADGE","LABEL","ITEM","CRYSTAL"];
/** Порядок чипов в фильтре REWARD_TYPE: 1-я строка / 2-я строка */
var REWARD_TYPE_FILTER_LAYOUT=[["BADGE","ITEM","LABEL"],["CRYSTAL",FILTER_EMPTY]];
var REWARD_TYPE_EMPTY_LABEL="Нет награды";
var contestListRewardTypes=new Set(REWARD_TYPE_FILTER_FALLBACK.concat([FILTER_EMPTY]));
/** Бизнес-блок: 4 кода каталога + остальные + пусто (по умолчанию все) */
var contestListBusinessBlocks=new Set(BUSINESS_BLOCK_NAMED.concat([FILTER_BB_OTHER,FILTER_EMPTY]));
/** Дата фильтра списка (YYYY-MM-DD) или "" */
var contestListDate="";
/** Режим отметки конкурсов для выгрузки JSON/CSV */
var contestSelectMode=false;
/** id конкурсов, отмеченных для выгрузки (сохраняются при смене фильтра) */
var selectedContestIds=new Set();
/** Порог: длинный список → combobox с поиском */
var COMBOBOX_MIN_VARIANTS=16;
/** @type {Array<object>} записи архива удалённых конкурсов и частей */
var archiveEntries=[];
/** id открытой карточки архива или null */
var activeArchiveId=null;
/** Секция внутри карточки архива (CONTEST / GROUP / PAIR / …) */
var activeArchiveSection="";
/** Прокрутка к блоку после render (id элемента) */
var pendingScrollTarget="";
/** Чип P×N в шапке: фокус на блоке CONTEST_PERIOD на карточке конкурса */
var periodNavFocus=false;
/** @type {Array<{id:string,name:string,baseline:any,data:any}>} */
var contests=[];

var $=id=>document.getElementById(id);
var esc=s=>String(s??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
var clone=o=>JSON.parse(JSON.stringify(o));

function toast(msg){const t=$("toast");$("toast-text").textContent=msg;t.hidden=false;clearTimeout(toast._t);toast._t=setTimeout(()=>{t.hidden=true},1600)}
function isIsoDate(s){return /^\d{4}-\d{2}-\d{2}$/.test(String(s||"").trim())}
function dateYearStart(){return `${new Date().getFullYear()}-01-01`}
function dateYearEnd(){return `${new Date().getFullYear()}-12-31`}
function dateToday(){const d=new Date();return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,"0")}-${String(d.getDate()).padStart(2,"0")}`}
function dateMonthStart(y,m){return `${y}-${String(m+1).padStart(2,"0")}-01`}
function dateMonthEnd(y,m){const last=new Date(y,m+1,0).getDate();return `${y}-${String(m+1).padStart(2,"0")}-${String(last).padStart(2,"0")}`}
function markSvg(){return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"><path d="M5 13l4 4L19 7"/></svg>'}
function calSvg(){return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="5" width="18" height="16" rx="2"/><path d="M3 10h18"/><path d="M8 3v4"/><path d="M16 3v4"/></svg>'}

function fieldIndex(){const map={};for(const sec of catalog.sections){for(const f of sec.fields){map[sec.id+"::"+f.key]=f;map[f.key]=map[f.key]||f}}return map}
function meta(sectionId,key){const idx=fieldIndex();return idx[sectionId+"::"+key]||idx[key]||{key,label:key,description:"",kind:"text",variants:[],default:"",allow_empty:true}}
var KIND_LABELS={dropdown:"Выбор из списка",dropdown_custom:"Список + свой вариант",text:"Свободный текст",number:"Число",list:"Массив значений",json:"JSON формат {[ ]}",date:"Дата (формат YYYY-MM-DD)"};
var KIND_SHORT={dropdown:"Список",dropdown_custom:"Список+",text:"Текст",number:"Число",list:"Массив",json:"JSON",date:"Дата"};
function kindLabel(kind){return KIND_LABELS[kind]||kind}
function kindShort(kind){return KIND_SHORT[kind]||kind}
/** true = можно оставить пустым (по каталогу или opts для pick). */
function fieldAllowEmpty(f, opts){
  opts=opts||{};
  if(Array.isArray(opts.pickVariants)) return !!opts.pickAllowEmpty;
  return !!f.allow_empty;
}
function emptyPillHtml(f, opts){
  const allow=fieldAllowEmpty(f,opts);
  if(allow) return `<span class="empty-pill is-opt" data-tip="Можно не указывать — значение не требуется">можно не указывать</span>`;
  return `<span class="empty-pill is-req" data-tip="Требуется указать: поле нельзя оставлять пустым">требуется указать</span>`;
}
/** Признак наличия ключа в JSON (не пустота значения). */
function isJsonKeyMeta(f){
  return !!(f && Object.prototype.hasOwnProperty.call(f,"json_required"));
}
function jsonRequiredPillHtml(f){
  if(!isJsonKeyMeta(f)) return "";
  const on=fieldJsonRequired(f);
  if(on) return `<span class="empty-pill is-json-req" data-tip="Ключ обязателен в JSON: при экспорте всегда присутствует. Пустое значение — отдельно флаг «можно не указывать»">ключ обязателен</span>`;
  return `<span class="empty-pill is-json-opt" data-tip="Ключ может отсутствовать в JSON: при пустом значении ключ не пишется">ключ может отсутствовать</span>`;
}
function tipFor(f, opts){
  const parts=[];
  if(f.description)parts.push(f.description);
  const kind=f.kind||"text";
  parts.push("Формат: "+kindLabel(kind));
  parts.push(fieldAllowEmpty(f,opts)?"Можно не указывать: да":"Требуется указать: нельзя оставлять пустым");
  if(isJsonKeyMeta(f)){
    parts.push(fieldJsonRequired(f)?"Ключ в JSON: обязателен":"Ключ в JSON: может отсутствовать");
  }
  if((f.variants||[]).length){
    const labs=f.variant_labels||[];
    parts.push("Варианты: "+f.variants.map((v,i)=>{
      const lab=String(labs[i]||"").trim();
      return lab?`${lab} (${v})`:v;
    }).join(" · "));
  }
  if(f.json_target)parts.push("JSON: "+f.json_target);
  return parts.join("\n");
}
function labelForVariant(f, value){
  const vs=f.variants||[];
  const i=vs.indexOf(value);
  if(i<0) return "";
  return String((f.variant_labels||[])[i]||"").trim();
}
function chipFaceHtml(value, label){
  const lab=String(label||"").trim();
  if(lab) return `<span class="default-chip__text"><span class="default-chip__label">${esc(lab)}</span><span class="default-chip__code">${esc(value)}</span></span>`;
  return `<span class="default-chip__label">${esc(value)}</span>`;
}
/** Рекомендуемое число пар связь+награда по типу (не жёсткий лимит). */
function recommendedBadges(t){t=String(t||"").trim().toUpperCase();if(t==="ИНДИВИДУАЛЬНЫЙ"||t==="ИНДИВИДУАЛЬНЫЙ НАКОПИТЕЛЬНЫЙ")return 1;return 3}
function maxBadges(t){return recommendedBadges(t)}
function typeBadgeHint(t){const n=recommendedBadges(t);const name=String(t||"не выбран").trim()||"не выбран";if(n===1)return{n,text:`Тип «${name}» → обычно 1 награда BADGE (можно добавить больше)`};return{n,text:`Тип «${name}» → обычно ${n} награды (рекомендация, не лимит)`}}

/** Порядок заполнения админки: группы + span сетки 12. */
var CONTEST_LAYOUT=[
  {title:"1. Тип конкурса",hint:"Выберите в начале: от типа — рекомендуемое число пар связь+награда (не жёсткий лимит).",items:[{key:"CONTEST_TYPE",span:12,hero:true},{key:"BUSINESS_STATUS",span:4},{key:"TARGET_TYPE",span:4},{key:"CONTEST_SUBJECT",span:4}]},
  {title:"2. Код и название",hint:"Код связывает таблицы GROUP / LINK / INDICATOR / SCHEDULE. Код — ⅓ ширины, название — ⅔ рядом.",items:[{key:"CONTEST_CODE",span:4},{key:"FULL_NAME",span:8}]},
  {title:"3. Период конкурса",hint:"Даты начала и окончания конкурса.",items:[{key:"CREATE_DT",span:6},{key:"CLOSE_DT",span:6}]},
  {title:"4. Описание",hint:"Текст для карточки турнира.",items:[{key:"CONTEST_DESCRIPTION",span:12}]},
  {title:"5. Продукт и блоки",hint:"Что участвует в конкурсе и в каких блоках.",items:[{key:"PRODUCT_GROUP",span:4},{key:"PRODUCT",span:4},{key:"BUSINESS_BLOCK",span:4},{key:"SHOW_INDICATOR",span:12}]},
  {title:"6. Правила победы",hint:"Как считаем победителей и показатель.",items:[{key:"FACTOR_MARK_TYPE",span:6},{key:"CONTEST_INDICATOR_METHOD",span:6},{key:"CONTEST_FACTOR_METHOD",span:6},{key:"CALC_TYPE",span:6}]},
  {title:"7. План",hint:"Как задаётся и сравнивается план.",items:[{key:"PLAN_METHOD_CODE",span:6},{key:"PLAN_MOD_METOD",span:6},{key:"FACTOR_MATCH",span:6},{key:"PLAN_MOD_VALUE",span:6}]},
  {title:"8. Обновление",hint:"Частота и постобработка.",items:[{key:"SOURCE_UPD_FREQUENCY",span:6},{key:"FACT_POST_PROCESSING",span:6}]},
  {title:"9. Периоды расчёта",hint:"JSON-массив CONTEST_PERIOD — блок ниже: несколько наборов period_code / criterion_mark_type / criterion_mark_value.",items:[]},
];
var FEATURE_LAYOUT=[
  {title:"1. Среда и отображение",hint:"Среда, округление, ёмкость, маскирование.",items:[{key:"CONTEST_FEATURE.vid",span:6},{key:"CONTEST_FEATURE.accuracy",span:3},{key:"CONTEST_FEATURE.capacity",span:3},{key:"CONTEST_FEATURE.masking",span:6},{key:"CONTEST_FEATURE.minNumber",span:6}]},
  {title:"2. Награждение",hint:"Момент и тип награждения, аватар, командность.",items:[{key:"CONTEST_FEATURE.momentRewarding",span:6},{key:"CONTEST_FEATURE.typeRewarding",span:6},{key:"CONTEST_FEATURE.avatarShow",span:6},{key:"CONTEST_FEATURE.tournamentTeam",span:6}]},
  {title:"3. Рассылки",hint:"Флаги и список рассылок турнира.",items:[{key:"CONTEST_FEATURE.tournamentStartMailing",span:4},{key:"CONTEST_FEATURE.tournamentEndMailing",span:4},{key:"CONTEST_FEATURE.tournamentLikeMailing",span:4},{key:"CONTEST_FEATURE.tournamentRewardingMailing",span:6},{key:"CONTEST_FEATURE.tournamentListMailing",span:6}]},
  {title:"4. Тексты и блоки CONTEST_FEATURE",hint:"Особенности, преференции, help. Бизнес-блок задаётся на странице конкурса.",items:[{key:"CONTEST_FEATURE.feature",span:12},{key:"CONTEST_FEATURE.preferences",span:12},{key:"CONTEST_FEATURE.businessBlock",span:6,locked:true,lockedHint:"Чтобы изменить — задайте бизнес-блок на странице конкурса"},{key:"CONTEST_FEATURE.helpCodeList",span:6}]},
  {title:"5. Видимость",hint:"Табельные, ТБ и ГОСБ — видимые / скрытые.",items:[{key:"CONTEST_FEATURE.persomanNumberVisible",span:6},{key:"CONTEST_FEATURE.persomanNumberHidden",span:6},{key:"CONTEST_FEATURE.tbVisible",span:6},{key:"CONTEST_FEATURE.tbHidden",span:6},{key:"CONTEST_FEATURE.gosbVisible",span:6},{key:"CONTEST_FEATURE.gosbHidden",span:6}]},
];
var REWARD_LAYOUT=[
  {title:"Основное",hint:"Тип награды, название, условие, стоимость и описание.",items:[
    {key:"REWARD_TYPE",span:4},
    {key:"FULL_NAME",span:8},
    {key:"REWARD_CONDITION",span:3},
    {key:"REWARD_COST",span:3},
    {key:"REWARD_DESCRIPTION",span:12}
  ]},
];
var ADD_LAYOUT=[
  {title:"1. Приоритет и роль",hint:"Слот места, мастер-бейдж, уровень рекомендации.",items:[{key:"REWARD_ADD_DATA.priority",span:3},{key:"REWARD_ADD_DATA.masterBadge",span:3},{key:"REWARD_ADD_DATA.recommendationLevel",span:3},{key:"REWARD_ADD_DATA.parentRewardCode",span:3}]},
  {title:"2. Правила и флаги",hint:"Правило получения и поведение награды.",items:[{key:"REWARD_ADD_DATA.rewardRule",span:12},{key:"REWARD_ADD_DATA.nftFlg",span:3},{key:"REWARD_ADD_DATA.outstanding",span:3},{key:"REWARD_ADD_DATA.rewardAgainGlobal",span:3},{key:"REWARD_ADD_DATA.rewardAgainTournament",span:3},{key:"REWARD_ADD_DATA.hidden",span:3},{key:"REWARD_ADD_DATA.hiddenRewardList",span:3},{key:"REWARD_ADD_DATA.refreshOldNews",span:3},{key:"REWARD_ADD_DATA.tournamentTeam",span:3}]},
  {title:"3. Новости и файлы",hint:"Тексты новостей, арт, сезон.",items:[{key:"REWARD_ADD_DATA.teamNews",span:12},{key:"REWARD_ADD_DATA.singleNews",span:12},{key:"REWARD_ADD_DATA.fileName",span:4},{key:"REWARD_ADD_DATA.seasonItem",span:4},{key:"REWARD_ADD_DATA.newsType",span:4},{key:"REWARD_ADD_DATA.winCriterion",span:12}]},
  {title:"4. Списки REWARD_ADD_DATA",hint:"Преференции и массивы через ;. Бизнес-блок задаётся на странице конкурса.",items:[{key:"REWARD_ADD_DATA.preferences",span:12},{key:"REWARD_ADD_DATA.feature",span:12},{key:"REWARD_ADD_DATA.businessBlock",span:6,locked:true,lockedHint:"Чтобы изменить — задайте бизнес-блок на странице конкурса"},{key:"REWARD_ADD_DATA.helpCodeList",span:6}]},
];
var SCHEDULE_LAYOUT=[
  {title:"1. Коды и статус",hint:"Новые: TOURNAMENT = t_ + CONTEST_CODE. Если в коде нет t_+CONTEST_CODE — поле правится целиком (старый формат). В JSON полный код + TOURNAMENT_CODE_ENDING.",items:[
    {key:"TOURNAMENT_CODE",span:6,compositeKind:"tournament"},
    {key:"TOURNAMENT_STATUS",span:6},
    {key:"PERIOD_TYPE",span:12}
  ]},
  {title:"2. Даты турнира",hint:"Старт, финиш и подведение итогов.",items:[
    {key:"START_DT",span:4},
    {key:"END_DT",span:4},
    {key:"RESULT_DT",span:4}
  ]},
  {title:"3. Период плана",hint:"Окно планового периода.",items:[
    {key:"PLAN_PERIOD_START_DT",span:6},
    {key:"PLAN_PERIOD_END_DT",span:6}
  ]},
  {title:"4. Критерий и расчёт",hint:"Критерий участия/отбора, тип расчёта и TRN_INDICATOR_FILTER. TARGET_TYPE и FILTER_PERIOD_ARR — блоки ниже.",items:[
    {key:"CRITERION_MARK_TYPE",span:8},
    {key:"CRITERION_MARK_VALUE",span:4},
    {key:"CALC_TYPE",span:8},
    {key:"TRN_INDICATOR_FILTER",span:4}
  ]},
];

var INDICATOR_LAYOUT=[
  {title:"1. Код и название",hint:"Код конкурса — в заголовке. Код, имя и ID индикатора — в одной строке.",items:[
    {key:"INDICATOR_CODE",span:4},
    {key:"FULL_NAME",span:4},
    {key:"N",span:4}
  ]},
  {title:"2. Тип расчёта",hint:"Как считается показатель.",items:[
    {key:"INDICATOR_CALC_TYPE",span:4},
    {key:"INDICATOR_ADD_CALC_TYPE",span:4},
    {key:"CALC_TYPE",span:4},
    {key:"INDICATOR_AGG_FUNCTION",span:6},
    {key:"INDICATOR_WEIGHT",span:6}
  ]},
  {title:"3. Объект и порог",hint:"Что измеряем и как сравниваем.",items:[
    {key:"INDICATOR_MARK_TYPE",span:4},
    {key:"CONTESTANT_SELECTION",span:4},
    {key:"INDICATOR_OBJECT",span:4},
    {key:"INDICATOR_MATCH",span:6},
    {key:"INDICATOR_VALUE",span:3},
    {key:"CONTEST_CRITERION",span:3}
  ]},
  {title:"4. Фильтр",hint:"INDICATOR_FILTER собирается блоком элементов ниже (не сырой JSON).",items:[]},
];

var GROUP_LAYOUT=[
  {title:"1. Код группы",hint:"Код конкурса — в заголовке. Уровень и значение группы.",items:[
    {key:"GROUP_CODE",span:6},
    {key:"BASE_CALC_CODE",span:6},
    {key:"GROUP_VALUE",span:12}
  ]},
  {title:"2. Методы и пороги",hint:"Как считается группа и дополнительные критерии.",items:[
    {key:"GET_CALC_METHOD",span:4},
    {key:"GET_CALC_CRITERION",span:4},
    {key:"ADD_CALC_CRITERION",span:4},
    {key:"ADD_CALC_CRITERION_2",span:12}
  ]},
];

var LINK_LAYOUT=[
  {title:"Связь группы и награды",hint:"Обычные: r_ + CONTEST_CODE. ITEM: ITEM_ + окончание товара. Чужой формат — поле целиком. В JSON полный код + REWARD_CODE_ENDING.",items:[
    {key:"GROUP_CODE",span:6,pickFromGroups:true},
    {key:"REWARD_CODE",span:6,compositeKind:"reward"}
  ]},
];





function fieldsByKey(sectionId){
  const map={};for(const f of (catalog.sections.find(s=>s.id===sectionId)||{fields:[]}).fields){
    if(f.kind==="json") continue; // колонка-оболочка JSON — метаданные allow_empty в edit
    map[f.key]=f;
  }return map;
}
function fieldJsonRequired(f){return !f||f.json_required!==false}
function isEmptyRaw(raw){return raw===undefined||raw===null||String(raw).trim()===""}
/** allow_empty колонки kind=json: весь JSON может отсутствовать (пустая ячейка). */
function jsonColumnAllowEmpty(tableSectionId, columnKey){
  const f=((catalog.sections.find(s=>s.id===tableSectionId)||{fields:[]}).fields||[]).find(x=>x.key===columnKey&&x.kind==="json");
  return f?!!f.allow_empty:true;
}
function isSpodJsonEmpty(obj){
  if(obj==null) return true;
  if(Array.isArray(obj)) return !obj.length;
  if(typeof obj==="object") return !Object.keys(obj).length;
  return String(obj).trim()==="";
}


/** Компактные пилюли связанных кодов в заголовке панели (не карточки полей). */
function ctxPillsHtml(pills){
  if(!pills||!pills.length) return "";
  return `<div class="ctx-pills" aria-label="Связанные коды">${pills.map(p=>{
    const empty=!String(p.v||"").trim();
    return `<span class="ctx-pill${empty?" is-empty":""}" data-k="${esc(p.k||"")}" data-tip="${esc(p.tip||"")}"><span class="ctx-pill__k">${esc(p.k)}</span><span class="ctx-pill__v">${esc(empty?"—":p.v)}</span></span>`;
  }).join("")}</div>`;
}
function panelHeadHtml(title, pills){
  return `<div class="panel-head"><h2>${esc(title)}</h2>${ctxPillsHtml(pills)}</div>`;
}
function contestCtxPill(){
  const v=contestCodeOf();
  return {k:"CONTEST",v,tip:v?"Код конкурса из шага «Конкурс»":"Сначала задайте CONTEST_CODE на шаге «Конкурс»"};
}
function renderGrouped(sectionId, layout, getValue, setValue, pathOf, omitKeys, viewOpts){
  viewOpts=viewOpts||{};
  const forceLocked=!!viewOpts.locked;
  const forceHint=viewOpts.lockedHint||"";
  const map=fieldsByKey(sectionId);
  const omit=new Set(omitKeys||[]);
  const used=new Set(omit);
  const root=document.createElement("div");
  for(const g of layout){
    const card=document.createElement("section");card.className="group-card";
    card.innerHTML=`<div class="group-card__head"><h3 class="group-card__title">${esc(g.title)}</h3>${g.hint?`<p class="group-card__hint">${esc(g.hint)}</p>`:""}</div><div class="fields-grid"></div>`;
    const grid=card.querySelector(".fields-grid");
    for(const it of g.items){
      if(omit.has(it.key)) continue;
      const base=map[it.key];if(!base)continue;used.add(it.key);
      const cardEl=renderFieldCard(base,getValue(base),pathOf(base),v=>setValue(base,v),{
        span:it.span||12,hero:!!it.hero,locked:forceLocked||!!it.locked,pickVariants:forceLocked?null:(it.pickVariants||null),lockedHint:it.lockedHint||forceHint||"",
        emptyPickHint:it.emptyPickHint||"",pickAllowEmpty:!!it.pickAllowEmpty,compositeKind:forceLocked?"":(it.compositeKind||"")
      });
      grid.appendChild(cardEl);
    }
    if(grid.children.length)root.appendChild(card);
  }
  const rest=Object.keys(map).filter(k=>!used.has(k));
  if(rest.length){
    const card=document.createElement("section");card.className="group-card";
    card.innerHTML=`<div class="group-card__head"><h3 class="group-card__title">Прочее</h3><p class="group-card__hint">Поля каталога вне основной раскладки.</p></div><div class="fields-grid"></div>`;
    const grid=card.querySelector(".fields-grid");
    for(const k of rest){const f=map[k];grid.appendChild(renderFieldCard(f,getValue(f),pathOf(f),v=>setValue(f,v),{span:12,locked:forceLocked,lockedHint:forceHint||"Архив — только просмотр"}))}
    root.appendChild(card);
  }
  return root;
}
function typeCalloutHtml(){
  const t=data().contest.CONTEST_TYPE||"";
  const h=typeBadgeHint(t);
  const cur=data().badges.length;
  return `<div class="type-callout"><div><div class="type-callout__title">Тип задаёт число пар по умолчанию</div><div class="type-callout__meta">${esc(h.text)}. Сейчас пар связь+награда: ${cur}. Цепочка: Группы → Связи + награды.</div></div><div class="type-callout__badge">${cur}/${h.n}</div></div>`;
}
function normalizeStandsList(raw, fallback){
  const fb=Array.isArray(fallback)&&fallback.length?fallback:[DEFAULT_STAND];
  if(!Array.isArray(raw)||!raw.length) return fb.slice();
  const out=[];
  for(const s of raw){
    const t=String(s||"").trim().toUpperCase();
    if(t&&STANDS_KNOWN.includes(t)&&!out.includes(t)) out.push(t);
  }
  return out.length?out:fb.slice();
}
function contestItemStands(c){
  if(c&&Array.isArray(c.stands)&&c.stands.length) return normalizeStandsList(c.stands);
  const card=c&&c.data&&c.data.contest;
  if(card&&Array.isArray(card.stands)&&card.stands.length) return normalizeStandsList(card.stands);
  return [DEFAULT_STAND];
}
function rowStands(row, c){
  if(row&&Array.isArray(row.stands)&&row.stands.length) return normalizeStandsList(row.stands);
  return contestItemStands(c||cur());
}
function badgeStands(badge, c){
  if(badge&&Array.isArray(badge.stands)&&badge.stands.length) return normalizeStandsList(badge.stands);
  return contestItemStands(c||cur());
}
function standBadgeClass(stand){
  const u=String(stand||"").toUpperCase();
  if(u==="PSI") return "stand-badge--psi";
  if(u==="IFT") return "stand-badge--ift";
  return "stand-badge--prom";
}
function standBadgesHtml(stands, opts){
  opts=opts||{};
  const list=normalizeStandsList(stands);
  if(!list.length) return "";
  const cls="stand-badges"+(opts.inline?" stand-badges--inline":"");
  const title=opts.title?` title="${esc(opts.title)}"`:"";
  return `<span class="${cls}"${title}>`+list.map(s=>`<span class="stand-badge ${standBadgeClass(s)}">${esc(s)}</span>`).join("")+`</span>`;
}
function standRowEditorHtml(stands, opts){
  opts=opts||{};
  const tags=new Set(normalizeStandsList(stands));
  function btn(stand, extra){
    const on=tags.has(stand);
    return `<button type="button" class="stand-toggle ${extra||""}${on?" is-on":""}" data-stand-toggle="${stand}" aria-pressed="${on?"true":"false"}">${stand}</button>`;
  }
  const id=opts.id?` id="${esc(opts.id)}"`:"";
  const label=esc(opts.label||"Стенд строки");
  return `<div class="stand-editor"${id} data-stand-editor="1"><span class="stand-editor__label">${label}</span>${btn("PROM","stand-toggle--prom")}${btn("PSI","stand-toggle--psi")}</div>`;
}
function contestStandEditorHtml(c){
  return standRowEditorHtml(contestItemStands(c), {id:"contest-stand-editor", label:"Стенд конкурса"});
}
function toggleStandInSet(current, stand){
  const set=new Set(normalizeStandsList(current));
  if(set.has(stand)) set.delete(stand);
  else set.add(stand);
  if(!set.size) set.add(DEFAULT_STAND);
  return STANDS_KNOWN.filter(s=>set.has(s));
}
function syncStandEditorUi(host, stands){
  if(!host) return;
  const tags=new Set(normalizeStandsList(stands));
  host.querySelectorAll("[data-stand-toggle]").forEach(btn=>{
    const stand=btn.getAttribute("data-stand-toggle")||"";
    const on=tags.has(stand);
    btn.classList.toggle("is-on", on);
    btn.setAttribute("aria-pressed", on?"true":"false");
  });
}
function bindStandRowEditor(hostOrId, onChange){
  const host=typeof hostOrId==="string"?$(hostOrId):hostOrId;
  if(!host) return;
  host.querySelectorAll("[data-stand-toggle]").forEach(btn=>{
    btn.addEventListener("click",()=>{
      const stand=String(btn.getAttribute("data-stand-toggle")||"").toUpperCase();
      if(!stand||!STANDS_UI.includes(stand)) return;
      onChange(stand, host);
    });
  });
}
function bindContestStandEditor(){
  bindStandRowEditor("contest-stand-editor",(stand, host)=>{
    const c=cur();
    c.stands=toggleStandInSet(contestItemStands(c), stand);
    if(c.data&&c.data.contest) c.data.contest.stands=c.stands.slice();
    markContestEdited();
    persistLocal();
    syncStandEditorUi(host, c.stands);
    renderContestTabs();
    renderNav();
  });
}
function recomputeContestStandsFromData(c){
  const found=new Set();
  const d=c.data||{};
  for(const key of ["group","indicator","schedule","reward_link","badges"]){
    for(const row of d[key]||[]){
      if(row&&Array.isArray(row.stands)) row.stands.forEach(s=>found.add(String(s).toUpperCase()));
    }
  }
  if(d.contest&&Array.isArray(d.contest.stands)) d.contest.stands.forEach(s=>found.add(String(s).toUpperCase()));
  const order=STANDS_KNOWN.filter(s=>found.has(s));
  c.stands=order.length?order:[DEFAULT_STAND];
  if(d.contest) d.contest.stands=c.stands.slice();
}
function bindRowStandEditor(hostId, getRow, onUpdated){
  bindStandRowEditor(hostId,(stand, host)=>{
    const row=getRow();
    if(!row) return;
    row.stands=toggleStandInSet(row.stands, stand);
    const c=cur();
    recomputeContestStandsFromData(c);
    markContestEdited();
    persistLocal();
    syncStandEditorUi(host, row.stands);
    if(onUpdated) onUpdated(row.stands);
    renderContestTabs();
    renderNav();
  });
}
function ensureDataStands(data, contestStands){
  if(!data||typeof data!=="object") return;
  const tags=normalizeStandsList(contestStands);
  const def=tags[0]||DEFAULT_STAND;
  if(data.contest&&typeof data.contest==="object") data.contest.stands=tags.slice();
  for(const key of ["group","indicator","schedule","reward_link"]){
    const rows=data[key];
    if(!Array.isArray(rows)) continue;
    for(const row of rows){
      if(row&&typeof row==="object"&&!Array.isArray(row.stands)) row.stands=[def];
    }
  }
  if(Array.isArray(data.badges)){
    for(const b of data.badges){
      if(b&&typeof b==="object"&&!Array.isArray(b.stands)) b.stands=[def];
    }
  }
}
function migrateContestStands(c, projectMeta){
  if(!c||typeof c!=="object") return;
  const block=String((projectMeta&&projectMeta.block)||projectBlock||DEFAULT_STAND).toUpperCase();
  const fb=block==="PSI"?["PSI"]:[DEFAULT_STAND];
  c.stands=normalizeStandsList(c.stands, fb);
  if(c.data) ensureDataStands(c.data, c.stands);
}
function tagNewEntityStands(entity){
  if(!entity||typeof entity!=="object") return entity;
  if(!Array.isArray(entity.stands)) entity.stands=[DEFAULT_STAND];
  return entity;
}
function standFilterSetMatchesTags(fs, tags){
  if(!fs||!fs.size) return false;
  const list=Array.isArray(tags)?tags:[];
  const prom=list.includes("PROM");
  const psi=list.includes("PSI");
  if(fs.has("PROM")&&prom) return true;
  if(fs.has("PSI")&&psi) return true;
  if(fs.has(STAND_FILTER_PROM_ONLY)&&prom&&!psi) return true;
  if(fs.has(STAND_FILTER_PSI_ONLY)&&psi&&!prom) return true;
  return false;
}
function contestMatchesStandFilter(c){
  return standFilterSetMatchesTags(contestListStandFilter, contestItemStands(c));
}
function standFilterShowsAllRows(){
  return contestListStandFilter.has("PROM")&&contestListStandFilter.has("PSI");
}
function rowMatchesStandFilter(row, c, filterSet){
  const fs=filterSet||contestListStandFilter;
  if(!fs||!fs.size) return false;
  if(fs.has("PROM")&&fs.has("PSI")) return true;
  return standFilterSetMatchesTags(fs, rowStands(row, c));
}
function filterRowIndices(rows, c){
  if(!Array.isArray(rows)) return [];
  if(standFilterShowsAllRows()) return rows.map((_,i)=>i);
  return rows.map((r,i)=>i).filter(i=>rowMatchesStandFilter(rows[i], c));
}
function rowIncludedInExport(row, c, exportStands){
  const sf=exportStands||CSV_EXPORT_STAND_PROM;
  return rowStands(row, c).some(s=>sf.has(s));
}
function contestIncludedInExport(c, exportStands){
  const sf=exportStands||CSV_EXPORT_STAND_PROM;
  return contestItemStands(c).some(s=>sf.has(s));
}

