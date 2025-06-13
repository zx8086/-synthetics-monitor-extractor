// Couchbase Eventing Function for Synthetics Monitoring Dashboard
// This function processes raw monitoring data from Elastic Synthetics via Kafka
// and transforms it into time-series metrics, status documents, and alerts using references instead of embedding

// Required bindings:
// raw_events (source) -> _default.raw_events
// metrics_1m (target) -> processed.metrics_1m
// metrics_5m (target) -> processed.metrics_5m
// metrics_1h (target) -> processed.metrics_1h
// services (target) -> analytics.services
// departments (target) -> analytics.departments
// domains (target) -> analytics.domains
// current_state (target) -> analytics.current_state
// alerts (target) -> analytics.alerts
// error_logs (target) -> logs.error_logs

function OnUpdate(doc, meta) {
  // Skip if not a valid monitoring document
  if (!doc || !doc.id || !meta.id.startsWith("raw::")) {
    return;
  }

  try {
    // Extract key information from document
    const timestamp = doc.timestamp
      ? new Date(doc.timestamp).getTime()
      : Date.now();

    // Extract domain and department information
    const domainName = doc.businessContext?.domain || "unknown";
    const domainId = createSafeId(domainName);

    const departmentName = doc.businessContext?.department || "unknown";
    const departmentId = createSafeId(departmentName);

    // Use monitor name as the primary name, with serviceId derived from it
    const monitorName = doc.name || "Unnamed Monitor";
    const serviceId = createSafeId(monitorName);

    const endpoint = doc.url || "/";
    const status = normalizeStatus((doc.status || "").toLowerCase());
    const monitorId = doc.id;

    // Process the data flow using references instead of embedding
    
    // 1. Update service document first (this is our source of truth)
    updateServiceDocument(
      serviceId,
      monitorName,
      domainId,
      departmentId,
      status,
      timestamp,
      doc
    );

    // 2. Update department document with service reference
    updateDepartmentDocument(
      departmentId,
      departmentName,
      domainId,
      serviceId,
      status,
      timestamp
    );

    // 3. Update domain document with department references
    updateDomainDocument(
      domainId,
      domainName,
      departmentId,
      status,
      timestamp
    );

    // 4. Store time-series data for charts and historical analysis
    storeTimeSeriesData(
      timestamp,
      domainId,
      departmentId,
      serviceId,
      endpoint,
      status,
      doc
    );

    // 5. Handle alerts for down services
    if (status === "down") {
      createAlert(
        doc,
        meta.id,
        serviceId,
        monitorName,
        domainId,
        domainName,
        departmentId,
        departmentName
      );
    } else if (status === "up") {
      resolveAlert(serviceId, monitorId);
    }

    // 6. Delete the processed document to avoid reprocessing
    deleteDocument(meta.id);
  } catch (e) {
    logError("Error processing monitoring data", meta.id, e);
  }
}

// Utility function to format percentage values consistently
function formatPercentage(value) {
  // Handle different formats of percentage values
  let numValue = parseFloat(value);
  
  // If NaN, return default value
  if (isNaN(numValue)) {
    return 100.0;
  }
  
  // If greater than 100 with no decimal point, assume it's multiplied by 100
  if (numValue > 100 && !String(value).includes('.')) {
    numValue = numValue / 100;
  }
  
  // Ensure percentage is within valid range (0-100)
  numValue = Math.max(0, Math.min(100, numValue));
  
  // Format with 2 decimal places
  return parseFloat(numValue.toFixed(2));
}

// Create a safe ID from a name - standardizes identifiers
function createSafeId(name) {
  if (!name || typeof name !== "string") {
    return "unknown";
  }

  try {
    return name
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9_]+/g, "-")
      .replace(/-{2,}/g, "-")
      .replace(/^-|-$/g, "")
      .substring(0, 100);
  } catch (e) {
    return "unknown-" + Date.now().toString(36);
  }
}

// Normalize status values to consistent format
function normalizeStatus(status) {
  const statusMap = {
    up: "up",
    down: "down",
    degraded: "degraded",
    warning: "degraded",
    critical: "down",
    healthy: "up"
  };

  return statusMap[status] || "unknown";
}

// Update service document - primary record of service status
function updateServiceDocument(
  serviceId,
  serviceName,
  domainId,
  departmentId,
  status,
  timestamp,
  doc
) {
  try {
    // Create service key with hierarchical ID
    const serviceKey = `service::${serviceId}`;
    let serviceDoc = null;

    try {
      serviceDoc = services[serviceKey];
    } catch (e) {
      serviceDoc = null;
    }

    if (!serviceDoc) {
      // Create new service document
      serviceDoc = {
        id: serviceId,
        name: serviceName,
        doc_type: "service",
        domain_id: domainId,
        department_id: departmentId,
        status: "unknown",
        last_check: 0,
        consecutive_failures: 0,
        last_failure: null,
        last_success: null,
        created_at: timestamp,
        metrics: {
          response_time_ms: 0,
          availability: 100.0, // Store as decimal percentage
          error_rate: 0.0      // Store as decimal percentage
        },
        monitors: {}
      };
    }

    // Update monitor information
    if (!serviceDoc.monitors) {
      serviceDoc.monitors = {};
    }

    serviceDoc.monitors[doc.id] = {
      id: doc.id,
      name: serviceName,
      type: doc.type || "synthetic",
      status: status,
      last_check: timestamp
    };

    // Update service status based on all monitors
    serviceDoc.status = calculateOverallStatus(serviceDoc.monitors);
    serviceDoc.last_check = timestamp;

    // Update failure tracking
    if (status === "down") {
      serviceDoc.consecutive_failures = 
        (serviceDoc.consecutive_failures || 0) + 1;
      serviceDoc.last_failure = timestamp;
    } else {
      serviceDoc.consecutive_failures = 0;
      serviceDoc.last_success = timestamp;
    }

    // Update metrics
    if (!serviceDoc.metrics) {
      serviceDoc.metrics = {
        response_time_ms: 0,
        availability: 100.0,
        error_rate: 0.0
      };
    }

    // Extract metrics from document and ensure proper formatting
    serviceDoc.metrics.response_time_ms = safeNumber(
      doc.duration || (doc.http && doc.http.responseTime) || 0
    );
    serviceDoc.metrics.availability = status === "up" ? 100.0 : 0.0;
    serviceDoc.metrics.error_rate = status === "down" ? 100.0 : 0.0;

    // Store updated document
    services[serviceKey] = serviceDoc;

    // Also update in current_state for backward compatibility
    updateCurrentStateDocument(
      serviceId,
      serviceName,
      domainId,
      departmentId,
      status,
      timestamp,
      doc
    );
  } catch (e) {
    logError("Error updating service document", serviceId, e);
  }
}

// Update current state document for backward compatibility
function updateCurrentStateDocument(
  serviceId,
  serviceName,
  domainId,
  departmentId,
  status,
  timestamp,
  doc
) {
  try {
    const statusKey = `status::${serviceId}`;
    let statusDoc = null;

    try {
      statusDoc = current_state[statusKey];
    } catch (e) {
      statusDoc = null;
    }

    if (!statusDoc) {
      // Create new status document
      statusDoc = {
        id: serviceId,
        name: serviceName,
        doc_type: "monitor_state",
        domain_id: domainId,
        department_id: departmentId,
        current_status: "unknown",
        last_check: 0,
        consecutive_failures: 0,
        last_failure: null,
        last_success: null,
        current_metrics: {
          response_time_ms: 0,
          last_5min_avg: 0.0,
          last_hour_avg: 0.0,
          last_hour_availability: 100.0
        },
        alert_status: {
          active_alerts: [],
          sla_breach: false,
          last_alert: null
        },
        business_context: doc.businessContext || {},
        monitors: {}
      };
    }

    // Update monitor information
    if (!statusDoc.monitors) {
      statusDoc.monitors = {};
    }

    statusDoc.monitors[doc.id] = {
      id: doc.id,
      name: serviceName,
      type: doc.type || "synthetic",
      status: status,
      last_check: timestamp
    };

    // Update status fields
    statusDoc.last_check = timestamp;
    statusDoc.current_status = calculateOverallStatus(statusDoc.monitors);

    // Update failure tracking
    if (status === "down") {
      statusDoc.consecutive_failures = 
        (statusDoc.consecutive_failures || 0) + 1;
      statusDoc.last_failure = timestamp;
    } else {
      statusDoc.consecutive_failures = 0;
      statusDoc.last_success = timestamp;
    }

    // Update metrics
    if (!statusDoc.current_metrics) {
      statusDoc.current_metrics = {
        response_time_ms: 0,
        last_5min_avg: 0.0,
        last_hour_avg: 0.0,
        last_hour_availability: 100.0
      };
    }

    statusDoc.current_metrics.response_time_ms = safeNumber(
      doc.duration || (doc.http && doc.http.responseTime) || 0
    );

    // Store document
    current_state[statusKey] = statusDoc;
  } catch (e) {
    logError("Error updating current state document", serviceId, e);
  }
}

// Update department document with service references
function updateDepartmentDocument(
  departmentId,
  departmentName,
  domainId,
  serviceId,
  status,
  timestamp
) {
  try {
    const departmentKey = `department::${departmentId}`;
    let departmentDoc = null;

    try {
      departmentDoc = departments[departmentKey];
    } catch (e) {
      departmentDoc = null;
    }

    if (!departmentDoc) {
      // Create new department document
      departmentDoc = {
        id: departmentId,
        name: formatDepartmentName(departmentName),
        doc_type: "department",
        domain_id: domainId,
        status: "unknown",
        service_refs: {},
        metrics: {
          healthy: 0,
          warning: 0,
          critical: 0,
          total: 0
        },
        created_at: timestamp,
        updated_at: timestamp,
        trend: generateTrendData(95.0, 100.0) // Use percentages for trend data
      };
    }

    // Ensure service_refs object exists
    if (!departmentDoc.service_refs) {
      departmentDoc.service_refs = {};
    }

    // Update service reference
    departmentDoc.service_refs[serviceId] = {
      status: status,
      last_check: timestamp
    };

    // FIX: Reset counts and recalculate based on actual service refs
    if (!departmentDoc.metrics) {
      departmentDoc.metrics = {
        healthy: 0,
        warning: 0,
        critical: 0,
        total: 0
      };
    }

    const metrics = departmentDoc.metrics;
    
    // Reset counters
    metrics.healthy = 0;
    metrics.warning = 0;
    metrics.critical = 0;
    
    // Count services by status
    Object.keys(departmentDoc.service_refs).forEach(svcId => {
      const svcStatus = departmentDoc.service_refs[svcId].status;
      if (svcStatus === "up") {
        metrics.healthy++;
      } else if (svcStatus === "degraded") {
        metrics.warning++;
      } else if (svcStatus === "down") {
        metrics.critical++;
      }
    });
    
    // Ensure total count is accurate
    metrics.total = Object.keys(departmentDoc.service_refs).length;

    // Update department status based on metrics
    departmentDoc.status = 
      metrics.critical > 0 ? "critical" : 
      metrics.warning > 0 ? "warning" : "healthy";
    
    departmentDoc.updated_at = timestamp;

    // Update trend data with availability percentage
    const availabilityPercent = metrics.total > 0 ?
      (metrics.healthy / metrics.total * 100) : 100.0;
      
    updateTrendData(departmentDoc.trend, availabilityPercent, timestamp);

    // Store updated document
    departments[departmentKey] = departmentDoc;
  } catch (e) {
    logError("Error updating department document", departmentId, e);
  }
}

// Update domain document with department references
function updateDomainDocument(
  domainId,
  domainName,
  departmentId,
  status,
  timestamp
) {
  try {
    const domainKey = `domain::${domainId}`;
    let domainDoc = null;

    try {
      domainDoc = domains[domainKey];
    } catch (e) {
      domainDoc = null;
    }

    if (!domainDoc) {
      // Create new domain document
      domainDoc = {
        id: domainId,
        name: domainName,
        doc_type: "domain",
        department_refs: {},
        overall_status: "unknown",
        metrics: {
          healthy_departments: 0,
          warning_departments: 0,
          critical_departments: 0,
          total_departments: 0,
          availability_percent: 100.0 // Use decimal percentage
        },
        created_at: timestamp,
        updated_at: timestamp,
        trend: generateTrendData(95.0, 100.0) // Use percentages for trend data
      };
    }

    // Ensure department_refs object exists
    if (!domainDoc.department_refs) {
      domainDoc.department_refs = {};
    }

    // Get department document to extract its status
    const departmentKey = `department::${departmentId}`;
    let departmentDoc = null;
    let departmentStatus = "unknown";
    
    try {
      departmentDoc = departments[departmentKey];
      if (departmentDoc) {
        departmentStatus = departmentDoc.status;
      }
    } catch (e) {
      departmentStatus = "unknown";
    }

    // Update department reference
    domainDoc.department_refs[departmentId] = {
      status: departmentStatus,
      last_check: timestamp
    };

    // FIX: Reset counts and recalculate based on actual department refs
    if (!domainDoc.metrics) {
      domainDoc.metrics = {
        healthy_departments: 0,
        warning_departments: 0,
        critical_departments: 0,
        total_departments: 0,
        availability_percent: 100.0
      };
    }

    const metrics = domainDoc.metrics;
    
    // Reset counters
    metrics.healthy_departments = 0;
    metrics.warning_departments = 0;
    metrics.critical_departments = 0;
    
    // Count departments by status
    Object.keys(domainDoc.department_refs).forEach(deptId => {
      const deptStatus = domainDoc.department_refs[deptId].status;
      if (deptStatus === "healthy") {
        metrics.healthy_departments++;
      } else if (deptStatus === "warning") {
        metrics.warning_departments++;
      } else if (deptStatus === "critical") {
        metrics.critical_departments++;
      }
    });
    
    // Ensure total count is accurate
    metrics.total_departments = Object.keys(domainDoc.department_refs).length;

    // Calculate availability percentage (as a decimal percentage)
    if (metrics.total_departments > 0) {
      metrics.availability_percent = formatPercentage(
        (metrics.healthy_departments / metrics.total_departments) * 100
      );
    } else {
      metrics.availability_percent = 100.0; // Default if no departments
    }

    // Update overall domain status
    domainDoc.overall_status = 
      metrics.critical_departments > 0 ? "critical" : 
      metrics.warning_departments > 0 ? "warning" : "healthy";
    
    domainDoc.updated_at = timestamp;

    // Update trend data with availability percentage
    updateTrendData(domainDoc.trend, metrics.availability_percent, timestamp);

    // Store updated document
    domains[domainKey] = domainDoc;
  } catch (e) {
    logError("Error updating domain document", domainId, e);
  }
}

// Helper function to update trend data
function updateTrendData(trend, newValue, timestamp) {
  try {
    if (!Array.isArray(trend)) {
      trend = [];
    }
    
    // Keep last 20 points
    if (trend.length >= 20) {
      trend.shift();
    }
    
    // FIX: Ensure value is a properly formatted percentage
    const formattedValue = formatPercentage(newValue);
    
    // Add new data point with proper formatting
    trend.push({
      timestamp: timestamp,
      value: formattedValue
    });
  } catch (e) {
    logError("Error updating trend data", e);
  }
}

// Store time-series data in different time granularities
function storeTimeSeriesData(
  timestamp,
  domainId,
  departmentId,
  serviceId,
  endpoint,
  status,
  doc
) {
  try {
    // Calculate time buckets from document timestamp
    const minuteBucket = Math.floor(timestamp / 60000) * 60000;
    const fiveMinBucket = Math.floor(timestamp / 300000) * 300000;
    const hourBucket = Math.floor(timestamp / 3600000) * 3600000;

    // Store in 1-minute buckets (high resolution, short retention)
    storeMetricInBucket(
      timestamp,
      minuteBucket,
      domainId,
      departmentId,
      serviceId,
      endpoint,
      status,
      doc,
      metrics_1m,
      60000,
      60
    );

    // Only store in 5-min bucket if this is a new 5-min interval
    if (timestamp % 300000 < 60000) {
      storeMetricInBucket(
        timestamp,
        fiveMinBucket,
        domainId,
        departmentId,
        serviceId,
        endpoint,
        status,
        doc,
        metrics_5m,
        300000,
        12
      );
    }

    // Only store in hourly bucket if this is a new hour
    if (timestamp % 3600000 < 60000) {
      storeMetricInBucket(
        timestamp,
        hourBucket,
        domainId,
        departmentId,
        serviceId,
        endpoint,
        status,
        doc,
        metrics_1h,
        3600000,
        24
      );
    }
  } catch (e) {
    logError("Error in storeTimeSeriesData", domainId + ":" + serviceId, e);
  }
}

// Store metric in a specific time bucket
function storeMetricInBucket(
  timestamp,
  bucketTime,
  domainId,
  departmentId,
  serviceId,
  endpoint,
  status,
  doc,
  collection,
  interval,
  maxPoints
) {
  try {
    if (!collection) return;

    // Create a unique key using domain and service IDs
    const tsKey = `ts::${domainId}::${serviceId}::${bucketTime}`;
    let tsDoc = null;

    try {
      tsDoc = collection[tsKey];
    } catch (e) {
      tsDoc = null;
    }

    if (!tsDoc) {
      // Create new time-series document with references to domains/departments/services
      tsDoc = {
        doc_type: "time_series_metrics",
        domain_id: domainId,
        department_id: departmentId,
        service_id: serviceId,
        endpoint: endpoint,
        ts_start: bucketTime,
        ts_end: bucketTime + interval * maxPoints,
        ts_interval: interval,
        data_points: [],
        metrics: {
          response_time_ms: [],
          availability: [],
          http_status: [],
          error_count: []
        },
        aggregates: {
          avg_response_time: 0,
          p95_response_time: 0,
          availability_percent: 100.0,
          error_rate: 0.0,
          min_response_time: 0,
          max_response_time: 0
        },
        business_context: doc.businessContext || {},
        tags: Array.isArray(doc.tags) ? doc.tags.slice(0, 10) : [],
        metadata: {
          schema_version: "1.0",
          created_at: Date.now(),
          updated_at: Date.now(),
          ttl: calculateTTL(collection)
        }
      };
    }

    // Extract metrics from the document
    const responseTime = safeNumber(
      doc.duration || (doc.http && doc.http.responseTime) || 0
    );
    const isAvailable = status === "up" ? 1 : 0;
    const httpStatus = safeNumber(doc.http && doc.http.statusCode || 0);
    const errorCount = status === "down" ? 1 : 0;

    // Calculate bucket offset
    const bucketOffset = Math.floor((timestamp - bucketTime) / interval);

    // Find or create data point
    let pointIndex = -1;
    if (Array.isArray(tsDoc.data_points)) {
      for (let i = 0; i < tsDoc.data_points.length; i++) {
        if (tsDoc.data_points[i] && tsDoc.data_points[i].offset === bucketOffset) {
          pointIndex = i;
          break;
        }
      }
    }

    if (pointIndex >= 0) {
      // Update existing data point
      const point = tsDoc.data_points[pointIndex];
      point.count = (point.count || 1) + 1;
      point.rt =
        ((point.rt || 0) * (point.count - 1) + responseTime) / point.count;
      point.a = point.a && isAvailable;
      point.h = httpStatus;
      point.e = (point.e || 0) + errorCount;
      point.min_rt = Math.min(point.min_rt || responseTime, responseTime);
      point.max_rt = Math.max(point.max_rt || responseTime, responseTime);
      tsDoc.data_points[pointIndex] = point;
    } else {
      // Create new data point
      if (!Array.isArray(tsDoc.data_points)) {
        tsDoc.data_points = [];
      }

      tsDoc.data_points.push({
        t: timestamp,
        offset: bucketOffset,
        rt: responseTime,
        a: isAvailable,
        h: httpStatus,
        e: errorCount,
        count: 1,
        min_rt: responseTime,
        max_rt: responseTime
      });
    }

    // Update metrics arrays
    updateMetricsArrays(
      tsDoc,
      responseTime,
      isAvailable,
      httpStatus,
      errorCount
    );

    // Update aggregates
    updateTimeSeriesAggregates(tsDoc);

    // Update metadata
    tsDoc.metadata.updated_at = Date.now();

    // Compact data points if too many (limit array size)
    if (
      Array.isArray(tsDoc.data_points) &&
      tsDoc.data_points.length > maxPoints
    ) {
      tsDoc.data_points = tsDoc.data_points.slice(-maxPoints);
    }

    // Store document
    collection[tsKey] = tsDoc;
  } catch (e) {
    logError("Error in storeMetricInBucket", bucketTime, e);
  }
}

// Calculate TTL based on collection - define retention for each granularity
function calculateTTL(collection) {
  if (collection === metrics_1h) return 7776000; // 90 days for hourly data
  if (collection === metrics_5m) return 2592000; // 30 days for 5m data
  return 864000; // 10 days for 1m data
}

// Update metrics arrays safely with bounds checking
function updateMetricsArrays(
  tsDoc,
  responseTime,
  isAvailable,
  httpStatus,
  errorCount
) {
  const maxMetricsLength = 100;

  // Ensure metrics object exists
  if (!tsDoc.metrics) {
    tsDoc.metrics = {
      response_time_ms: [],
      availability: [],
      http_status: [],
      error_count: []
    };
  }

  // Helper function to safely update an array
  function updateArray(arrayName, value) {
    if (!Array.isArray(tsDoc.metrics[arrayName])) {
      tsDoc.metrics[arrayName] = [];
    }

    tsDoc.metrics[arrayName].push(value);

    if (tsDoc.metrics[arrayName].length > maxMetricsLength) {
      tsDoc.metrics[arrayName] = 
        tsDoc.metrics[arrayName].slice(-maxMetricsLength);
    }
  }

  updateArray("response_time_ms", responseTime);
  updateArray("availability", isAvailable);
  updateArray("http_status", httpStatus);
  updateArray("error_count", errorCount);
}

// Update time-series aggregates - calculate statistical values
function updateTimeSeriesAggregates(tsDoc) {
  if (
    !tsDoc.metrics ||
    !tsDoc.metrics.response_time_ms ||
    !Array.isArray(tsDoc.metrics.response_time_ms) ||
    tsDoc.metrics.response_time_ms.length === 0
  ) {
    return;
  }

  try {
    // Ensure aggregates object exists
    if (!tsDoc.aggregates) {
      tsDoc.aggregates = {
        avg_response_time: 0,
        p95_response_time: 0,
        availability_percent: 100.0,
        error_rate: 0.0,
        min_response_time: 0,
        max_response_time: 0
      };
    }

    // Calculate average response time
    let sum = 0;
    for (let i = 0; i < tsDoc.metrics.response_time_ms.length; i++) {
      sum += safeNumber(tsDoc.metrics.response_time_ms[i]);
    }
    tsDoc.aggregates.avg_response_time =
      parseFloat((sum / tsDoc.metrics.response_time_ms.length).toFixed(2));

    // Calculate availability percentage
    if (
      Array.isArray(tsDoc.metrics.availability) &&
      tsDoc.metrics.availability.length > 0
    ) {
      let availSum = 0;
      for (let i = 0; i < tsDoc.metrics.availability.length; i++) {
        availSum += safeNumber(tsDoc.metrics.availability[i]);
      }
      tsDoc.aggregates.availability_percent = formatPercentage(
        (availSum / tsDoc.metrics.availability.length) * 100
      );
    }

    // Calculate error rate
    if (
      Array.isArray(tsDoc.metrics.error_count) &&
      tsDoc.metrics.error_count.length > 0
    ) {
      let errorSum = 0;
      for (let i = 0; i < tsDoc.metrics.error_count.length; i++) {
        errorSum += safeNumber(tsDoc.metrics.error_count[i]);
      }
      tsDoc.aggregates.error_rate = formatPercentage(
        (errorSum / tsDoc.metrics.error_count.length) * 100
      );
    }

    // Calculate percentiles and min/max
    const sortedRt = [...tsDoc.metrics.response_time_ms].sort(
      (a, b) => safeNumber(a) - safeNumber(b)
    );
    if (sortedRt.length > 0) {
      const p95Index = Math.floor(sortedRt.length * 0.95);
      tsDoc.aggregates.p95_response_time = parseFloat(sortedRt[p95Index].toFixed(2)) || 0;
      tsDoc.aggregates.min_response_time = parseFloat(sortedRt[0].toFixed(2)) || 0;
      tsDoc.aggregates.max_response_time = parseFloat(sortedRt[sortedRt.length - 1].toFixed(2)) || 0;
    }
  } catch (e) {
    logError("Error updating time series aggregates", e);
  }
}

// Create alert for down services
function createAlert(
  doc,
  docId,
  serviceId,
  serviceName,
  domainId,
  domainName,
  departmentId,
  departmentName
) {
  try {
    const monitorId = doc.id;
    const timestamp = doc.timestamp
      ? new Date(doc.timestamp).getTime()
      : Date.now();

    // Create a stable alert ID for deduplication
    const alertId = `alert::${serviceId}::${monitorId}`;

    // Check if alert already exists
    let existingAlert = null;
    try {
      existingAlert = alerts[alertId];
    } catch (e) {
      existingAlert = null;
    }

    if (
      existingAlert &&
      existingAlert.metadata &&
      existingAlert.metadata.resolved === false
    ) {
      // Alert already exists - no need to create another
      return;
    }

    // Create alert document with references to service, domain, and department
    const alertDoc = {
      doc_type: "alert",
      id: alertId,
      monitor_id: monitorId,
      service_id: serviceId,
      service_name: serviceName,
      domain_id: domainId,
      domain_name: domainName,
      department_id: departmentId,
      department_name: departmentName,
      environment:
        doc.environment || (doc.businessContext && doc.businessContext.environment) || "production",
      criticality: (doc.businessContext && doc.businessContext.criticality) || "medium",
      status: "down",
      timestamp: new Date(timestamp).toISOString(),
      first_detected: Date.now(),
      message: `Monitor ${serviceName} is DOWN`,
      error_details: (doc.http && doc.http.statusCode)
        ? `HTTP Status: ${doc.http.statusCode}`
        : (doc.error && doc.error.message)
          ? doc.error.message
          : "No additional details",
      source_doc: docId,
      metadata: {
        created_at: Date.now(),
        resolved: false,
        resolved_at: null,
        acknowledged: false,
        acknowledged_by: null,
        acknowledged_at: null,
        ttl: 604800 // 7 days
      }
    };

    // Store alert
    alerts[alertId] = alertDoc;

    // Update service with alert reference
    updateServiceWithAlertRef(serviceId, alertId);
  } catch (e) {
    logError("Error creating alert", e);
  }
}

// Update service with alert reference
function updateServiceWithAlertRef(serviceId, alertId) {
  try {
    const serviceKey = `service::${serviceId}`;
    let serviceDoc = null;

    try {
      serviceDoc = services[serviceKey];
    } catch (e) {
      serviceDoc = null;
    }

    if (serviceDoc) {
      if (!serviceDoc.alert_status) {
        serviceDoc.alert_status = {
          active_alerts: [],
          sla_breach: false,
          last_alert: null
        };
      }

      if (!Array.isArray(serviceDoc.alert_status.active_alerts)) {
        serviceDoc.alert_status.active_alerts = [];
      }

      // Check if alert already exists in array
      let alertExists = false;
      for (let i = 0; i < serviceDoc.alert_status.active_alerts.length; i++) {
        if (serviceDoc.alert_status.active_alerts[i] === alertId) {
          alertExists = true;
          break;
        }
      }

      if (!alertExists) {
        serviceDoc.alert_status.active_alerts.push(alertId);
      }

      serviceDoc.alert_status.last_alert = Date.now();
      services[serviceKey] = serviceDoc;
    }

    // Also update in current_state for backward compatibility
    const statusKey = `status::${serviceId}`;
    let statusDoc = null;

    try {
      statusDoc = current_state[statusKey];
    } catch (e) {
      statusDoc = null;
    }

    if (statusDoc) {
      if (!statusDoc.alert_status) {
        statusDoc.alert_status = {
          active_alerts: [],
          sla_breach: false,
          last_alert: null
        };
      }

      if (!Array.isArray(statusDoc.alert_status.active_alerts)) {
        statusDoc.alert_status.active_alerts = [];
      }

      // Check if alert already exists in array
      let alertExists = false;
      for (let i = 0; i < statusDoc.alert_status.active_alerts.length; i++) {
        if (statusDoc.alert_status.active_alerts[i] === alertId) {
          alertExists = true;
          break;
        }
      }

      if (!alertExists) {
        statusDoc.alert_status.active_alerts.push(alertId);
      }

      statusDoc.alert_status.last_alert = Date.now();
      current_state[statusKey] = statusDoc;
    }
  } catch (e) {
    logError("Error in updateServiceWithAlertRef", serviceId, e);
  }
}

// Resolve an alert when service comes back up
function resolveAlert(serviceId, monitorId) {
  try {
    const alertId = `alert::${serviceId}::${monitorId}`;
    let existingAlert = null;

    try {
      existingAlert = alerts[alertId];
    } catch (e) {
      return;
    }

    if (
      existingAlert &&
      existingAlert.metadata &&
      existingAlert.metadata.resolved === false
    ) {
      existingAlert.metadata.resolved = true;
      existingAlert.metadata.resolved_at = Date.now();
      existingAlert.status = "resolved";

      // Store resolved alert
      alerts[alertId] = existingAlert;

      // Remove alert reference from service
      removeAlertRefFromService(serviceId, alertId);
      
      // Remove alert reference from current_state
      removeAlertRefFromCurrentState(serviceId, alertId);
    }
  } catch (e) {
    logError("Error resolving alert", e);
  }
}

// Remove alert reference from service
function removeAlertRefFromService(serviceId, alertId) {
  try {
    const serviceKey = `service::${serviceId}`;
    let serviceDoc = null;

    try {
      serviceDoc = services[serviceKey];
    } catch (e) {
      return;
    }
    
    if (
      serviceDoc &&
      serviceDoc.alert_status &&
      Array.isArray(serviceDoc.alert_status.active_alerts)
    ) {
      const newAlerts = [];
      for (let i = 0; i < serviceDoc.alert_status.active_alerts.length; i++) {
        if (serviceDoc.alert_status.active_alerts[i] !== alertId) {
          newAlerts.push(serviceDoc.alert_status.active_alerts[i]);
        }
      }
      
      serviceDoc.alert_status.active_alerts = newAlerts;
      services[serviceKey] = serviceDoc;
    }
  } catch (e) {
    logError("Error in removeAlertRefFromService", e);
  }
}

// Remove alert reference from current_state
function removeAlertRefFromCurrentState(serviceId, alertId) {
  try {
    const statusKey = `status::${serviceId}`;
    let statusDoc = null;

    try {
      statusDoc = current_state[statusKey];
    } catch (e) {
      return;
    }

    if (
      statusDoc &&
      statusDoc.alert_status &&
      Array.isArray(statusDoc.alert_status.active_alerts)
    ) {
      const newAlerts = [];
      for (let i = 0; i < statusDoc.alert_status.active_alerts.length; i++) {
        if (statusDoc.alert_status.active_alerts[i] !== alertId) {
          newAlerts.push(statusDoc.alert_status.active_alerts[i]);
        }
      }
      
      statusDoc.alert_status.active_alerts = newAlerts;
      current_state[statusKey] = statusDoc;
    }
  } catch (e) {
    logError("Error in removeAlertRefFromCurrentState", e);
  }
}

// Calculate overall status based on monitors
function calculateOverallStatus(monitors) {
  if (!monitors || typeof monitors !== "object") {
    return "unknown";
  }

  try {
    let hasDown = false;
    let hasDegraded = false;
    
    for (const monitorId in monitors) {
      const monitor = monitors[monitorId];
      if (monitor && monitor.status === "down") {
        hasDown = true;
      } else if (monitor && monitor.status === "degraded") {
        hasDegraded = true;
      }
    }

    if (hasDown) return "down";
    if (hasDegraded) return "degraded";
    return "up";
  } catch (e) {
    logError("Error in calculateOverallStatus", e);
    return "unknown";
  }
}// Generate trend data for departments/domains
function generateTrendData(minValue = 95.0, maxValue = 100.0) {
  try {
    const points = [];
    const now = Date.now();
    const baseValue = Math.random() * 5 + 95; // 95-100 range for percentages
    
    for (let i = 0; i < 20; i++) {
      const timeOffset = i * 72 * 60000; // 72 minutes apart = 24 hours total
      const value = Math.max(
        minValue,
        Math.min(maxValue, baseValue + (Math.random() * 2 - 1))
      );

      points.push({
        timestamp: now - timeOffset,
        value: formatPercentage(value)
      });
    }

    return points;
  } catch (e) {
    logError("Error generating trend data", e);
    return [
      { timestamp: Date.now(), value: 99.5 },
      { timestamp: Date.now() - 86400000, value: 99.5 }
    ];
  }
}

// Format department name for display
function formatDepartmentName(department) {
  if (!department) return "Unknown";

  try {
    // Split by underscores or dashes and capitalize each word
    const words = department.split(/[_-]/);
    const formattedWords = [];
    
    for (let i = 0; i < words.length; i++) {
      const word = words[i];
      if (word.length > 0) {
        formattedWords.push(word.charAt(0).toUpperCase() + word.slice(1));
      }
    }
    
    return formattedWords.join(" ");
  } catch (e) {
    logError("Error formatting department name", e);
    return String(department).charAt(0).toUpperCase() + String(department).slice(1);
  }
}

// Safely convert to number
function safeNumber(value, defaultValue = 0) {
  try {
    const num = Number(value);
    return isNaN(num) ? defaultValue : num;
  } catch (e) {
    return defaultValue;
  }
}

// Delete processed document
function deleteDocument(docId) {
  try {
    raw_events[docId] = null;
  } catch (e) {
    logError("Error deleting document", docId, e);
  }
}

// Log errors to error_logs collection
function logError(message, context, error) {
  try {
    const errorId = `error::${Date.now()}::${Math.floor(Math.random() * 1000000)}`;
    error_logs[errorId] = {
      message: String(message || "").substring(0, 200),
      context: context !== undefined ? String(context).substring(0, 200) : "",
      error: error !== undefined ? String(error).substring(0, 500) : "",
      timestamp: new Date().toISOString(),
      metadata: {
        created_at: Date.now(),
        ttl: 2592000 // 30 days
      }
    };
  } catch (e) {
    // If even error logging fails, just log to console
    log("Error logging failed:", e);
  }
}

// Document deletion handler
function OnDelete(meta) {
  // Optional cleanup for deleted documents
  try {
    log("Document deleted:", meta.id);
  } catch (e) {
    // Ignore errors in deletion handler
  }
}

// Function to repair existing documents with formatting issues
function repairDocuments() {
  try {
    log("Starting document repair process...");
    
    // Repair domain documents
    repairDomainDocuments();
    
    // Repair department documents
    repairDepartmentDocuments();
    
    // Repair service documents
    repairServiceDocuments();
    
    log("Document repair process completed.");
  } catch (e) {
    logError("Error in repairDocuments", e);
  }
}

// Repair domain documents
function repairDomainDocuments() {
  try {
    // Use N1QL to get all domain IDs
    const statement = "SELECT RAW META().id FROM monitoring.analytics.domains WHERE doc_type = 'domain'";
    const queryResult = query(statement);
    
    if (!queryResult || !Array.isArray(queryResult)) {
      log("No domain documents found or query failed");
      return;
    }
    
    log(`Found ${queryResult.length} domain documents to repair`);
    
    queryResult.forEach(id => {
      try {
        const domainDoc = domains[id];
        if (!domainDoc) return;
        
        // Fix availability percentage
        if (domainDoc.metrics && domainDoc.metrics.availability_percent) {
          domainDoc.metrics.availability_percent = 
            formatPercentage(domainDoc.metrics.availability_percent);
        }
        
        // Fix trend data
        if (Array.isArray(domainDoc.trend)) {
          domainDoc.trend = domainDoc.trend.map(point => {
            return {
              timestamp: point.timestamp,
              value: formatPercentage(point.value)
            };
          });
        }
        
        // Save repaired document
        domains[id] = domainDoc;
        log(`Repaired domain document: ${id}`);
      } catch (e) {
        logError("Error repairing domain document", id, e);
      }
    });
  } catch (e) {
    logError("Error in repairDomainDocuments", e);
  }
}

// Repair department documents
function repairDepartmentDocuments() {
  try {
    // Use N1QL to get all department IDs
    const statement = "SELECT RAW META().id FROM monitoring.analytics.departments WHERE doc_type = 'department'";
    const queryResult = query(statement);
    
    if (!queryResult || !Array.isArray(queryResult)) {
      log("No department documents found or query failed");
      return;
    }
    
    log(`Found ${queryResult.length} department documents to repair`);
    
    queryResult.forEach(id => {
      try {
        const deptDoc = departments[id];
        if (!deptDoc) return;
        
        // Reset counters
        let healthy = 0;
        let warning = 0;
        let critical = 0;
        
        // Count services by status if service_refs exists
        if (deptDoc.service_refs) {
          Object.keys(deptDoc.service_refs).forEach(svcId => {
            const svcStatus = deptDoc.service_refs[svcId].status;
            if (svcStatus === "up") {
              healthy++;
            } else if (svcStatus === "degraded") {
              warning++;
            } else if (svcStatus === "down") {
              critical++;
            }
          });
          
          // Ensure metrics object exists
          if (!deptDoc.metrics) {
            deptDoc.metrics = {
              healthy: 0,
              warning: 0,
              critical: 0,
              total: 0
            };
          }
          
          // Update metrics
          deptDoc.metrics.healthy = healthy;
          deptDoc.metrics.warning = warning;
          deptDoc.metrics.critical = critical;
          deptDoc.metrics.total = Object.keys(deptDoc.service_refs).length;
        }
        
        // Fix trend data
        if (Array.isArray(deptDoc.trend)) {
          deptDoc.trend = deptDoc.trend.map(point => {
            return {
              timestamp: point.timestamp,
              value: formatPercentage(point.value)
            };
          });
        }
        
        // Save repaired document
        departments[id] = deptDoc;
        log(`Repaired department document: ${id}`);
      } catch (e) {
        logError("Error repairing department document", id, e);
      }
    });
  } catch (e) {
    logError("Error in repairDepartmentDocuments", e);
  }
}
// Repair service documents
function repairServiceDocuments() {
  try {
    // Use N1QL to get all service IDs
    const statement = "SELECT RAW META().id FROM monitoring.analytics.services WHERE doc_type = 'service'";
    const queryResult = query(statement);
    
    if (!queryResult || !Array.isArray(queryResult)) {
      log("No service documents found or query failed");
      return;
    }
    
    log(`Found ${queryResult.length} service documents to repair`);
    
    queryResult.forEach(id => {
      try {
        const serviceDoc = services[id];
        if (!serviceDoc) return;
        
        // Fix metrics
        if (serviceDoc.metrics) {
          if (serviceDoc.metrics.availability) {
            serviceDoc.metrics.availability = formatPercentage(serviceDoc.metrics.availability);
          }
          
          if (serviceDoc.metrics.error_rate) {
            serviceDoc.metrics.error_rate = formatPercentage(serviceDoc.metrics.error_rate);
          }
        }
        
        // Save repaired document
        services[id] = serviceDoc;
        log(`Repaired service document: ${id}`);
      } catch (e) {
        logError("Error repairing service document", id, e);
      }
    });
  } catch (e) {
    logError("Error in repairServiceDocuments", e);
  }
}



