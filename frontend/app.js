const elements = {
  apiBase: document.getElementById("apiBase"),
  saveApi: document.getElementById("saveApi"),
  cycleId: document.getElementById("cycleId"),
  invoiceFile: document.getElementById("invoiceFile"),
  erpFile: document.getElementById("erpFile"),
  itbFile: document.getElementById("itbFile"),
  uploadInvoice: document.getElementById("uploadInvoice"),
  uploadErp: document.getElementById("uploadErp"),
  uploadItb: document.getElementById("uploadItb"),
  processCycle: document.getElementById("processCycle"),
  cycleStatus: document.getElementById("cycleStatus"),
  poFile: document.getElementById("poFile"),
  uploadPo: document.getElementById("uploadPo"),
  staticStatus: document.getElementById("staticStatus"),
  inputCycle: document.getElementById("inputCycle"),
  inputTable: document.getElementById("inputTable"),
  loadInput: document.getElementById("loadInput"),
  inputTableWrap: document.getElementById("inputTableWrap"),
  txnCycle: document.getElementById("txnCycle"),
  txnTable: document.getElementById("txnTable"),
  loadTxn: document.getElementById("loadTxn"),
  txnTableWrap: document.getElementById("txnTableWrap"),
  certCycle: document.getElementById("certCycle"),
  loadCerts: document.getElementById("loadCerts"),
  certTableWrap: document.getElementById("certTableWrap"),
  adminSql: document.getElementById("adminSql"),
  runQuery: document.getElementById("runQuery"),
  queryTableWrap: document.getElementById("queryTableWrap"),
};

const defaultApiBase = window.location.origin;
const storedApiBase = localStorage.getItem("apiBase");
if (storedApiBase) {
  elements.apiBase.value = storedApiBase;
} else {
  elements.apiBase.value = defaultApiBase;
}

function getApiBase() {
  const raw = elements.apiBase.value.trim();
  const base = raw.length ? raw : defaultApiBase;
  return base.replace(/\/$/, "");
}

function setStatus(element, message, isError = false) {
  element.textContent = message;
  element.style.color = isError ? "#dc2626" : "#6b7280";
}

async function uploadFile(endpoint, file) {
  if (!file) {
    throw new Error("Please select a CSV file");
  }
  const formData = new FormData();
  formData.append("file", file);
  const response = await fetch(`${getApiBase()}${endpoint}`, {
    method: "POST",
    body: formData,
  });
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || "Upload failed");
  }
  return response.json();
}

async function fetchJson(endpoint, options = {}) {
  const response = await fetch(`${getApiBase()}${endpoint}`, options);
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || "Request failed");
  }
  return response.json();
}

function renderTable(container, rows) {
  if (!rows || rows.length === 0) {
    container.innerHTML = "<p class=\"status\">No data returned.</p>";
    return;
  }
  const columns = Object.keys(rows[0]);
  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const headerRow = document.createElement("tr");
  columns.forEach((col) => {
    const th = document.createElement("th");
    th.textContent = col;
    headerRow.appendChild(th);
  });
  thead.appendChild(headerRow);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    columns.forEach((col) => {
      const td = document.createElement("td");
      td.textContent = row[col] ?? "";
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  container.innerHTML = "";
  container.appendChild(table);
}

function renderCertificationTable(container, rows) {
  if (!rows || rows.length === 0) {
    container.innerHTML = "<p class=\"status\">No ERP transactions found.</p>";
    return;
  }

  const columns = [
    "id",
    "itb_no",
    "ln_itm_id",
    "cost_id",
    "submitted_acwp",
    "authorized_cost_amount",
    "certified_without_fee",
    "certification_status",
  ];

  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const headerRow = document.createElement("tr");
  columns.concat(["actions"]).forEach((col) => {
    const th = document.createElement("th");
    th.textContent = col;
    headerRow.appendChild(th);
  });
  thead.appendChild(headerRow);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    columns.forEach((col) => {
      const td = document.createElement("td");
      if (col === "certified_without_fee") {
        const input = document.createElement("input");
        input.type = "number";
        input.step = "0.01";
        input.value = row[col] ?? 0;
        input.dataset.id = row.id;
        td.appendChild(input);
      } else if (col === "certification_status") {
        const input = document.createElement("input");
        input.type = "text";
        input.value = row[col] ?? "";
        input.dataset.statusId = row.id;
        td.appendChild(input);
      } else {
        td.textContent = row[col] ?? "";
      }
      tr.appendChild(td);
    });

    const actionTd = document.createElement("td");
    const button = document.createElement("button");
    button.className = "btn ghost";
    button.textContent = "Save";
    button.addEventListener("click", async () => {
      const amountInput = tr.querySelector("input[data-id]");
      const statusInput = tr.querySelector("input[data-status-id]");
      try {
        const payload = {
          certified_without_fee: Number(amountInput.value || 0),
          certification_status: statusInput.value || undefined,
        };
        await fetchJson(`/txn/erp/${row.id}/certification`, {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        button.textContent = "Saved";
        setTimeout(() => {
          button.textContent = "Save";
        }, 1500);
      } catch (error) {
        alert(error.message);
      }
    });
    actionTd.appendChild(button);
    tr.appendChild(actionTd);

    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  container.innerHTML = "";
  container.appendChild(table);
}

function requireCycle(input) {
  const value = input.value.trim();
  if (!value) {
    throw new Error("Please enter the ITB / Cycle ID");
  }
  return value;
}

elements.saveApi.addEventListener("click", () => {
  const base = getApiBase();
  localStorage.setItem("apiBase", base);
  alert("API base saved.");
});

elements.uploadInvoice.addEventListener("click", async () => {
  try {
    const cycle = requireCycle(elements.cycleId);
    setStatus(elements.cycleStatus, "Uploading invoice file...");
    await uploadFile(`/cycles/${encodeURIComponent(cycle)}/upload/invoice-information`, elements.invoiceFile.files[0]);
    setStatus(elements.cycleStatus, "Invoice information uploaded.");
  } catch (error) {
    setStatus(elements.cycleStatus, error.message, true);
  }
});

elements.uploadErp.addEventListener("click", async () => {
  try {
    const cycle = requireCycle(elements.cycleId);
    setStatus(elements.cycleStatus, "Uploading ERP file...");
    await uploadFile(`/cycles/${encodeURIComponent(cycle)}/upload/erp-actuals`, elements.erpFile.files[0]);
    setStatus(elements.cycleStatus, "ERP actuals uploaded.");
  } catch (error) {
    setStatus(elements.cycleStatus, error.message, true);
  }
});

elements.uploadItb.addEventListener("click", async () => {
  try {
    const cycle = requireCycle(elements.cycleId);
    setStatus(elements.cycleStatus, "Uploading ITB cost performance...");
    await uploadFile(`/cycles/${encodeURIComponent(cycle)}/upload/itb-cost-performance`, elements.itbFile.files[0]);
    setStatus(elements.cycleStatus, "ITB cost performance uploaded.");
  } catch (error) {
    setStatus(elements.cycleStatus, error.message, true);
  }
});

elements.processCycle.addEventListener("click", async () => {
  try {
    const cycle = requireCycle(elements.cycleId);
    setStatus(elements.cycleStatus, "Processing cycle...");
    await fetchJson(`/cycles/${encodeURIComponent(cycle)}/process`, { method: "POST" });
    setStatus(elements.cycleStatus, `Cycle ${cycle} processed.`);
  } catch (error) {
    setStatus(elements.cycleStatus, error.message, true);
  }
});

elements.uploadPo.addEventListener("click", async () => {
  try {
    setStatus(elements.staticStatus, "Uploading PO master...");
    await uploadFile("/po-master", elements.poFile.files[0]);
    setStatus(elements.staticStatus, "PO master uploaded.");
  } catch (error) {
    setStatus(elements.staticStatus, error.message, true);
  }
});

elements.loadInput.addEventListener("click", async () => {
  try {
    const cycle = requireCycle(elements.inputCycle);
    const table = elements.inputTable.value;
    const data = await fetchJson(`/input/${table}?itb_no=${encodeURIComponent(cycle)}`);
    renderTable(elements.inputTableWrap, data.records);
  } catch (error) {
    elements.inputTableWrap.innerHTML = `<p class=\"status\" style=\"color:#dc2626;\">${error.message}</p>`;
  }
});

elements.loadTxn.addEventListener("click", async () => {
  try {
    const cycle = requireCycle(elements.txnCycle);
    const table = elements.txnTable.value;
    const data = await fetchJson(`/txn/${table}?itb_no=${encodeURIComponent(cycle)}`);
    renderTable(elements.txnTableWrap, data.records);
  } catch (error) {
    elements.txnTableWrap.innerHTML = `<p class=\"status\" style=\"color:#dc2626;\">${error.message}</p>`;
  }
});

elements.loadCerts.addEventListener("click", async () => {
  try {
    const cycle = requireCycle(elements.certCycle);
    const data = await fetchJson(`/txn/erp?itb_no=${encodeURIComponent(cycle)}`);
    renderCertificationTable(elements.certTableWrap, data.records);
  } catch (error) {
    elements.certTableWrap.innerHTML = `<p class=\"status\" style=\"color:#dc2626;\">${error.message}</p>`;
  }
});

elements.runQuery.addEventListener("click", async () => {
  try {
    const sql = elements.adminSql.value.trim();
    if (!sql) {
      throw new Error("Enter a SQL query first.");
    }
    const data = await fetchJson("/admin/query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sql }),
    });
    renderTable(elements.queryTableWrap, data.records);
  } catch (error) {
    elements.queryTableWrap.innerHTML = `<p class=\"status\" style=\"color:#dc2626;\">${error.message}</p>`;
  }
});
