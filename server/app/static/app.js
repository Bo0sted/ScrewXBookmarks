"use strict";
(() => {
  const $$ = (root, sel) => [...(root.matches && root.matches(sel) ? [root] : []), ...root.querySelectorAll(sel)];
  const viewer = document.getElementById("viewer");
  const viewerOpen = () => !viewer.hidden;

  // ---------- Smart loading: only media within ~a screen of the viewport is loaded ----------
  function attach(el) {
    if (el.dataset.poster && !el.getAttribute("poster")) el.poster = el.dataset.poster;
    if (el.dataset.src && !el.getAttribute("src")) {
      if (el.tagName === "VIDEO") el.preload = "metadata";
      el.src = el.dataset.src;
    }
  }

  function detach(video) {
    // Far off-screen videos give their memory/decoder back; the poster stays.
    if (!video.getAttribute("src")) return;
    video.pause();
    video.removeAttribute("src");
    video.load();
  }

  const lazy = new IntersectionObserver((entries) => {
    for (const e of entries) {
      if (e.isIntersecting) attach(e.target);
      else if (e.target.tagName === "VIDEO") detach(e.target);
    }
  }, { rootMargin: "1200px 0px" });

  // ---------- Autoplay like X: play muted while mostly visible, pause + rewind when gone ----------
  const visibleVideos = new Set();
  const autoplay = new IntersectionObserver((entries) => {
    for (const e of entries) {
      const v = e.target;
      const visible = e.isIntersecting &&
        (e.intersectionRatio >= 0.5 || e.intersectionRect.height >= window.innerHeight * 0.5);
      if (visible) {
        visibleVideos.add(v);
        attach(v);
        if (v.paused && !viewerOpen()) v.play().catch(() => {});
      } else {
        visibleVideos.delete(v);
        v.pause();
        if (!e.isIntersecting && v.getAttribute("src")) v.currentTime = 0;
      }
    }
  }, { threshold: [0, 0.1, 0.25, 0.5, 0.75, 1] });

  // ---------- Infinite scroll: the next chunk loads well before you reach the bottom ----------
  const nearBottom = new IntersectionObserver((entries) => {
    if (entries.some((e) => e.isIntersecting)) loadMore();
  }, { rootMargin: "1600px 0px" });

  let loading = null;
  function loadMore() {
    const btn = document.querySelector(".more[data-next]");
    if (!btn) return Promise.resolve(false);
    if (loading) return loading;
    btn.disabled = true;
    btn.textContent = "Loading…";
    loading = fetch(btn.dataset.next)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.text();
      })
      .then((html) => {
        const tpl = document.createElement("template");
        tpl.innerHTML = html;
        const nodes = [...tpl.content.children];
        btn.replaceWith(...nodes);
        nodes.forEach(setup);
        updateButtons();
        return true;
      })
      .catch(() => {
        btn.disabled = false;
        btn.textContent = "Couldn't load more — tap to retry";
        return false;
      })
      .finally(() => { loading = null; });
    return loading;
  }

  function setup(root) {
    $$(root, "[data-src]").forEach((el) => lazy.observe(el));
    $$(root, "video").forEach((v) => autoplay.observe(v));
    $$(root, ".more[data-next]").forEach((b) => nearBottom.observe(b));
  }

  // ---------- Viewer: full quality, ←/→ through the post's media then the next post ----------
  const stage = viewer.querySelector(".v-stage");
  const caption = viewer.querySelector(".v-caption");
  const prevBtn = viewer.querySelector(".v-prev");
  const nextBtn = viewer.querySelector(".v-next");
  let current = null;

  const items = () => [...document.querySelectorAll("main .mi")];

  function updateButtons() {
    if (!current) return;
    const list = items();
    const i = list.indexOf(current);
    prevBtn.disabled = i <= 0;
    nextBtn.disabled = i >= list.length - 1 && !document.querySelector(".more[data-next]");
  }

  function show(el) {
    current = el;
    const list = items();
    const i = list.indexOf(el);
    const d = el.dataset;
    const ctx = el.closest("[data-link]") || el;

    let media;
    if (d.type === "photo") {
      media = new Image();
      media.alt = "";
      media.src = d.full;
    } else {
      media = document.createElement("video");
      media.src = d.full;
      media.loop = true;
      media.playsInline = true;
      media.controls = d.type !== "gif";
      media.muted = d.type === "gif";
      media.play().catch(() => { media.muted = true; media.play().catch(() => {}); });
    }
    media.className = "v-media";
    stage.replaceChildren(media);

    const count = Number(ctx.dataset.count || 1);
    const text = document.createElement("span");
    text.textContent = (ctx.dataset.caption || "") + (count > 1 ? ` · ${Number(d.idx) + 1}/${count}` : "");
    const link = document.createElement("a");
    link.href = ctx.dataset.link;
    link.target = "_blank";
    link.rel = "noopener";
    link.textContent = "view on X ↗";
    caption.replaceChildren(text, " · ", link);
    updateButtons();

    // Preload neighbours at full quality, and keep the next chunk loaded ahead.
    for (const j of [i + 1, i + 2, i - 1]) {
      const n = list[j];
      if (n && n.dataset.type === "photo") new Image().src = n.dataset.full;
    }
    if (list.length - i <= 8) loadMore();
  }

  async function step(dir) {
    if (!current) return;
    const i = items().indexOf(current) + dir;
    if (dir > 0 && i >= items().length) await loadMore();
    const list = items();
    if (i >= 0 && i < list.length) show(list[i]);
  }

  function open(el) {
    visibleVideos.forEach((v) => v.pause());
    viewer.hidden = false;
    document.body.classList.add("noscroll");
    show(el);
  }

  function close() {
    viewer.hidden = true;
    stage.replaceChildren();
    document.body.classList.remove("noscroll");
    if (current) current.scrollIntoView({ block: "center" });
    current = null;
    visibleVideos.forEach((v) => v.play().catch(() => {}));
  }

  // ---------- Toasts, and buttons that ask the server to do something (delete, restore, catch-up) ----------
  const toasts = document.getElementById("toasts");
  function toast(text, ok) {
    const t = document.createElement("div");
    t.className = "toast" + (ok === true ? " ok" : ok === false ? " err" : "");
    t.textContent = text;
    toasts.append(t);
    setTimeout(() => t.remove(), ok === false ? 9000 : 5000);
  }

  const ACTIONS = {
    delete: (id) => `/post/${id}/delete`,
    restore: (id) => `/deleted/${id}/restore`,
    catchup: () => "/sync/catchup",
  };

  async function act(btn) {
    if (btn.disabled) return;
    const { action, id } = btn.dataset;
    btn.disabled = true;
    if (action === "catchup") toast("Syncing latest reposts…", null);
    let res;
    try {
      const r = await fetch(ACTIONS[action](id), { method: "POST" });
      const j = await r.json().catch(() => ({ message: `HTTP ${r.status}` }));
      res = { ...j, ok: r.ok && j.ok !== false };
    } catch (err) {
      res = { ok: false, message: `Request failed: ${err}` };
    }
    toast(res.message, res.ok);
    if (res.ok && action === "catchup" && res.new) return setTimeout(() => location.reload(), 1500);
    if (res.ok && action !== "catchup") return btn.closest("[data-row]").remove();
    btn.disabled = false;
  }

  document.addEventListener("click", (e) => {
    const actionBtn = e.target.closest("[data-action]");
    if (actionBtn) {
      act(actionBtn);
      return;
    }
    const more = e.target.closest(".more[data-next]");
    if (more) {
      loadMore();
      return;
    }
    const mi = e.target.closest("main .mi");
    if (mi) {
      e.preventDefault(); // folder thumbnails sit inside a link to the author page
      open(mi);
    }
  });

  viewer.addEventListener("click", (e) => {
    if (e.target.closest(".v-prev")) return step(-1);
    if (e.target.closest(".v-next")) return step(1);
    if (e.target.closest(".v-close")) return close();
    if (e.target.closest(".v-media, .v-caption > *")) return;
    close(); // clicked outside the media (including empty space around the caption)
  });

  document.addEventListener("keydown", (e) => {
    if (!viewerOpen()) return;
    if (e.key === "ArrowRight") { e.preventDefault(); step(1); }
    else if (e.key === "ArrowLeft") { e.preventDefault(); step(-1); }
    else if (e.key === "Escape") close();
  });

  let touchX = null;
  viewer.addEventListener("touchstart", (e) => {
    touchX = e.touches.length === 1 ? e.touches[0].clientX : null;
  }, { passive: true });
  viewer.addEventListener("touchend", (e) => {
    if (touchX === null) return;
    const dx = e.changedTouches[0].clientX - touchX;
    touchX = null;
    if (Math.abs(dx) > 50) step(dx < 0 ? 1 : -1);
  }, { passive: true });

  setup(document);
})();
