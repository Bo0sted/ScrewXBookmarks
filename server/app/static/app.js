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
    zoom = { s: 1, x: 0, y: 0 };
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
    media.draggable = false;
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

  // ---------- Viewer zoom: the wheel zooms toward the cursor, dragging pans while zoomed ----------
  const MAX_ZOOM = 8;
  let zoom = { s: 1, x: 0, y: 0 };
  let drag = null;
  let panned = false;

  function applyZoom(media) {
    media.style.transform = zoom.s === 1 ? "" : `translate(${zoom.x}px, ${zoom.y}px) scale(${zoom.s})`;
    media.classList.toggle("zoomed", zoom.s > 1);
  }

  stage.addEventListener("wheel", (e) => {
    const media = stage.querySelector(".v-media");
    if (!media) return;
    e.preventDefault();
    const dy = e.deltaMode === 1 ? e.deltaY * 33 : e.deltaY; // Firefox may report lines, not pixels
    const s = Math.min(MAX_ZOOM, Math.max(1, zoom.s * Math.exp(-dy * 0.0015)));
    if (s === zoom.s) return;
    // Cursor position relative to the unzoomed media; keep the point under the cursor where it is.
    const r = media.getBoundingClientRect();
    const ox = e.clientX - (r.left - zoom.x);
    const oy = e.clientY - (r.top - zoom.y);
    zoom = s === 1 ? { s: 1, x: 0, y: 0 }
      : { s, x: ox - (ox - zoom.x) * s / zoom.s, y: oy - (oy - zoom.y) * s / zoom.s };
    applyZoom(media);
  }, { passive: false });

  stage.addEventListener("pointerdown", (e) => {
    panned = false;
    const media = e.target.closest(".v-media");
    if (!media || zoom.s === 1 || e.button !== 0) return;
    e.preventDefault();
    drag = { id: e.pointerId, x: e.clientX - zoom.x, y: e.clientY - zoom.y, media };
    media.setPointerCapture(e.pointerId);
  });
  stage.addEventListener("pointermove", (e) => {
    if (!drag || e.pointerId !== drag.id) return;
    zoom.x = e.clientX - drag.x;
    zoom.y = e.clientY - drag.y;
    panned = true;
    applyZoom(drag.media);
  });
  stage.addEventListener("pointerup", () => { drag = null; });
  stage.addEventListener("pointercancel", () => { drag = null; });

  // ---------- Per-post actions menu ----------
  function closeMenus(except) {
    document.querySelectorAll(".menu:not([hidden])").forEach((m) => { if (m !== except) m.hidden = true; });
  }

  // ---------- Server calls and the shared modal ----------
  async function api(method, url, body) {
    try {
      const opts = { method };
      if (body !== undefined) {
        opts.headers = { "Content-Type": "application/json" };
        opts.body = JSON.stringify(body);
      }
      const r = await fetch(url, opts);
      const j = await r.json().catch(() => ({ message: `HTTP ${r.status}` }));
      return { ...j, ok: r.ok && j.ok !== false };
    } catch (err) {
      return { ok: false, message: `Request failed: ${err}` };
    }
  }

  const h = (tag, props = {}, ...kids) => {
    const el = Object.assign(document.createElement(tag), props);
    el.append(...kids);
    return el;
  };

  const modal = document.getElementById("modal");
  const modalBody = modal.querySelector(".modal-body");
  function openModal(...nodes) {
    modalBody.replaceChildren(...nodes);
    modal.hidden = false;
  }
  function closeModal() {
    modal.hidden = true;
    modalBody.replaceChildren();
  }
  modal.addEventListener("click", (e) => {
    if (e.target === modal || e.target.closest(".modal-close")) closeModal();
  });

  // ---------- Tags: the 🏷 modal on posts and users; every change applies immediately ----------
  const byName = (a, b) => a.name.localeCompare(b.name, undefined, { sensitivity: "base" });

  function chip(tag) {
    const c = h("a", { className: "tag-chip", href: `/?tags=${tag.id}`, textContent: tag.name });
    c.dataset.tag = tag.id;
    return c;
  }

  // Mirrors a change in the posts' "Tags" footers. A user's tag goes on / comes off all their posts.
  function updateFooters(kind, id, tag, on) {
    const sel = kind === "posts" ? `article[data-post="${id}"]` : `article[data-author="${id}"]`;
    document.querySelectorAll(sel).forEach((post) => {
      const foot = post.querySelector(".tags-foot");
      const existing = foot.querySelector(`.tag-chip[data-tag="${tag.id}"]`);
      if (on && !existing) {
        const c = chip(tag);
        const after = [...foot.querySelectorAll(".tag-chip")].find((x) => byName({ name: x.textContent }, tag) > 0);
        foot.insertBefore(c, after || null);
      } else if (!on && existing) {
        existing.remove();
      }
      foot.hidden = !foot.querySelector(".tag-chip");
    });
  }

  async function openTagger(btn) {
    const kind = btn.dataset.tagger;
    const id = btn.dataset.id;
    const base = `/api/${kind}/${id}/tags`;
    const res = await api("GET", base);
    if (!res.ok) return toast(res.message, false);
    const tags = res.tags;
    const applied = new Set(res.applied);

    const input = h("input", { type: "search", placeholder: "Find or create a tag…", autocomplete: "off" });
    const list = h("div", { className: "tag-list" });

    async function setTag(tag, on) {
      const r = await api(on ? "POST" : "DELETE", `${base}/${tag.id}`);
      if (r.ok) {
        if (on) applied.add(tag.id); else applied.delete(tag.id);
        updateFooters(kind, id, tag, on);
      } else {
        toast(r.message, false);
      }
      render();
    }

    async function createAndAdd() {
      const r = await api("POST", "/api/tags", { name: input.value });
      if (!r.ok) return toast(r.message, false);
      tags.push(r.tag);
      tags.sort(byName);
      input.value = "";
      await setTag(r.tag, true);
    }

    function render() {
      const q = input.value.trim().toLowerCase();
      const rows = tags.filter((t) => t.name.toLowerCase().includes(q)).map((t) => {
        const cb = h("input", { type: "checkbox", checked: applied.has(t.id) });
        cb.addEventListener("change", () => {
          cb.disabled = true;
          setTag(t, cb.checked);
        });
        return h("label", { className: "tag-row" }, cb, t.name);
      });
      if (q && !tags.some((t) => t.name.toLowerCase() === q)) {
        const create = h("button", { className: "tag-create" }, `+ Create “${input.value.trim()}”`);
        create.addEventListener("click", createAndAdd);
        rows.unshift(create);
      }
      if (!rows.length) rows.push(h("p", { className: "muted" }, "No tags yet. Type a name to create one."));
      list.replaceChildren(...rows);
    }

    input.addEventListener("input", render);
    input.addEventListener("keydown", (e) => {
      if (e.key !== "Enter") return;
      e.preventDefault();
      const q = input.value.trim().toLowerCase();
      if (!q) return;
      const tag = tags.find((t) => t.name.toLowerCase() === q);
      if (!tag) return createAndAdd();
      input.value = "";
      if (!applied.has(tag.id)) setTag(tag, true); else render();
    });

    const title = kind === "authors"
      ? `Tags for ${btn.dataset.label} · applies to each of their posts`
      : "Tags for this post";
    render();
    openModal(h("h3", {}, title), input, list);
    input.focus();
  }

  // ---------- Tags page: add, rename, delete (with confirmation) ----------
  const tagNew = document.getElementById("tag-new");
  if (tagNew) {
    tagNew.addEventListener("submit", async (e) => {
      e.preventDefault();
      const r = await api("POST", "/api/tags", { name: tagNew.elements.tag.value });
      if (!r.ok) return toast(r.message, false);
      location.reload();
    });
  }

  async function renameTag(row) {
    const input = row.querySelector(".tag-name");
    if (input.value.trim() === row.dataset.name) return;
    const r = await api("PATCH", `/api/tags/${row.dataset.tagId}`, { name: input.value });
    if (!r.ok) {
      input.value = row.dataset.name;
      return toast(r.message, false);
    }
    row.dataset.name = input.value = r.tag.name;
    toast(`Renamed to “${r.tag.name}”`, true);
  }

  function confirmDeleteTag(row) {
    const name = row.dataset.name;
    const cancel = h("button", {}, "Cancel");
    const del = h("button", { className: "btn-danger" }, "Delete");
    cancel.addEventListener("click", closeModal);
    del.addEventListener("click", async () => {
      del.disabled = true;
      const r = await api("DELETE", `/api/tags/${row.dataset.tagId}`);
      closeModal();
      if (!r.ok) return toast(r.message, false);
      row.remove();
      toast(`Deleted tag “${name}”`, true);
    });
    openModal(
      h("h3", {}, `Delete tag “${name}”?`),
      h("p", { className: "muted" }, "It's removed from every post and user it's on. This can't be undone."),
      h("div", { className: "modal-actions" }, cancel, del),
    );
  }

  document.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && e.target.matches(".tag-name")) {
      e.preventDefault();
      renameTag(e.target.closest("tr"));
    }
  });

  // ---------- Tag filter (Recent): each checkbox applies right away; the dropdown stays open ----------
  const tagFilter = document.getElementById("tag-filter");
  if (tagFilter) {
    try {
      if (sessionStorage.getItem("reopenTagFilter")) {
        sessionStorage.removeItem("reopenTagFilter");
        tagFilter.open = true;
      }
    } catch {}
    tagFilter.addEventListener("change", () => {
      try { sessionStorage.setItem("reopenTagFilter", "1"); } catch {}
      tagFilter.closest("form").submit();
    });
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
    const res = await api("POST", ACTIONS[action](id));
    toast(res.message, res.ok);
    if (res.ok && action === "catchup" && res.new) return setTimeout(() => location.reload(), 1500);
    if (res.ok && action !== "catchup") return btn.closest("[data-row]").remove();
    btn.disabled = false;
  }

  document.addEventListener("click", (e) => {
    const menuBtn = e.target.closest(".menu-btn");
    if (menuBtn) {
      const menu = menuBtn.nextElementSibling;
      closeMenus(menu);
      menu.hidden = !menu.hidden;
      return;
    }
    closeMenus();
    document.querySelectorAll("details.dropdown[open]").forEach((d) => { if (!d.contains(e.target)) d.open = false; });
    const tagger = e.target.closest("[data-tagger]");
    if (tagger) {
      openTagger(tagger);
      return;
    }
    const renameBtn = e.target.closest("[data-tag-rename]");
    if (renameBtn) {
      renameTag(renameBtn.closest("tr"));
      return;
    }
    const deleteTagBtn = e.target.closest("[data-tag-delete]");
    if (deleteTagBtn) {
      confirmDeleteTag(deleteTagBtn.closest("tr"));
      return;
    }
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
    if (panned) { panned = false; return; } // end of a drag, not a click
    if (e.target.closest(".v-prev")) return step(-1);
    if (e.target.closest(".v-next")) return step(1);
    if (e.target.closest(".v-close")) return close();
    if (e.target.closest(".v-media, .v-caption > *")) return;
    close(); // clicked outside the media (including empty space around the caption)
  });

  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") {
      closeMenus();
      if (!modal.hidden) return closeModal();
    }
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
    if (touchX === null || zoom.s > 1) return; // while zoomed, a swipe pans instead of changing media
    const dx = e.changedTouches[0].clientX - touchX;
    touchX = null;
    if (Math.abs(dx) > 50) step(dx < 0 ? 1 : -1);
  }, { passive: true });

  setup(document);
})();
