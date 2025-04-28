"""
Web-based dashboard for TradeGuard Health service.

This module provides a FastAPI web server that displays the internal state
of the application in a browser.
"""

import threading
import time
import json
from typing import Dict, Any, List, Optional
from datetime import datetime
import uvicorn
import logging

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware

from src.config.config import Config
from src.utils import log_util
from src.utils.datetime_utils import DateTimeUtils

logger = log_util.get_logger()

uvicorn_access_logger = logging.getLogger("uvicorn.access")
uvicorn_access_logger.setLevel(logging.WARNING)


app = FastAPI(title="TradeGuard Health Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

dashboard_state = {
    "jobs_state": {},
    "dca_jobs": {},
    "liq_jobs": {},
    "job_to_user_map": {},
    "positions_state": {},
    "equity_state": {},
    "last_update": time.time(),
    "startup_time": datetime.now().timestamp()
}

DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>TradeGuard Health Dashboard</title>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
        .tab {
            padding: 10px 20px;
            cursor: pointer;
            display: inline-block;
            border: 1px solid #ccc;
            border-bottom: none;
            border-radius: 4px 4px 0 0;
            background-color: #f1f1f1;
        }
        
        .tab.active {
            background-color: #fff;
            border-bottom: 1px solid white;
            margin-bottom: -1px;
        }
        
        .tab-content {
            display: none;
            padding: 20px;
            border: 1px solid #ccc;
            border-radius: 0 0 4px 4px;
            background-color: white;
        }
        
        .tab-content.active {
            display: block;
        }
        
        .card {
            background-color: white;
            border-radius: 4px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.12);
            padding: 16px;
            margin-bottom: 16px;
        }
        
        .card h2 {
            font-size: 1.25rem;
            font-weight: bold;
            margin-bottom: 12px;
        }
        
        table {
            width: 100%;
            border-collapse: collapse;
        }
        
        th, td {
            padding: 8px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }
        
        tr:hover {
            background-color: #f5f5f5;
        }
        
        .positive-pnl {
            color: green;
        }
        
        .negative-pnl {
            color: red;
        }
        
        /* Modal styles */
        .modal {
            display: none;
            position: fixed;
            z-index: 100;
            left: 0;
            top: 0;
            width: 100%;
            height: 100%;
            overflow: auto;
            background-color: rgba(0,0,0,0.4);
        }
        
        .modal-content {
            background-color: #fefefe;
            margin: 10% auto;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 4px 8px rgba(0,0,0,0.2);
            width: 80%;
            max-width: 800px;
        }
        
        .close {
            color: #aaa;
            float: right;
            font-size: 28px;
            font-weight: bold;
            cursor: pointer;
        }
        
        .close:hover {
            color: black;
        }
        
        /* Refreshing animation */
        @keyframes pulse {
            0% { opacity: 1; }
            50% { opacity: 0.5; }
            100% { opacity: 1; }
        }
        
        .pulse {
            animation: pulse 2s infinite;
        }
        
        /* Raw data tab */
        pre.raw-data {
            max-height: 600px;
            overflow: auto;
            background-color: #f5f5f5;
            padding: 10px;
            border-radius: 4px;
            font-family: monospace;
            font-size: 12px;
        }
        
        .raw-data-tab {
            transition: background-color 0.2s;
            border: 1px solid #e2e8f0;
            border-bottom: none;
        }
        
        .raw-data-tab.active {
            border-bottom: 1px solid #f8fafc;
            margin-bottom: -1px;
        }
        
        .raw-data-tab:hover {
            background-color: #e2e8f0 !important;
        }
        
        .raw-data-content {
            border: 1px solid #e2e8f0;
            border-radius: 0 0 4px 4px;
            padding: 1rem;
            background-color: #f8fafc;
        }
    </style>
    <script>
        // Store current state data
        let currentData = null;
        
        // Auto-refresh data every 2 seconds
        function refreshData() {
            fetch('/api/state')
                .then(response => response.json())
                .then(data => {
                    // Store current data for use in other functions
                    currentData = data;
                    
                    // Update timestamp
                    const lastUpdate = new Date(data.last_update * 1000);
                    document.getElementById('lastUpdate').textContent = lastUpdate.toLocaleString();
                    
                    // Update summary statistics
                    updateSummaryStats(data);
                    
                    // Update jobs tab
                    updateJobsTab(data);
                    
                    // Update positions tab
                    updatePositionsTab(data);
                    
                    // Update equity tab
                    updateEquityTab(data);
                    
                    // Update raw data tab
                    updateRawDataTab(data);
                    
                    // Add event listeners for job detail clicks
                    addJobDetailListeners();
                })
                .catch(error => console.error('Error fetching data:', error));
        }
        
        function updateSummaryStats(data) {
            // Calculate summary statistics
            const totalJobs = Object.values(data.jobs_state).reduce((sum, jobs) => sum + Object.keys(jobs).length, 0);
            const totalUsers = Object.keys(data.jobs_state).length;
            const totalDcaJobs = Object.values(data.dca_jobs).reduce((sum, jobs) => sum + Object.keys(jobs).length, 0);
            const totalLiqJobs = Object.values(data.liq_jobs).reduce((sum, jobs) => sum + Object.keys(jobs).length, 0);
            
            // Count positions
            let totalPositions = 0;
            for (const userId in data.positions_state) {
                totalPositions += Object.keys(data.positions_state[userId]).length;
            }
            
            // Count running jobs
            const runningJobs = Object.values(data.jobs_state).reduce((sum, jobs) => {
                return sum + Object.values(jobs).filter(j => j.status === 'Running').length;
            }, 0);
            
            // Calculate uptime
            const uptime = Math.floor((Date.now() / 1000) - data.startup_time);
            const days = Math.floor(uptime / 86400);
            const hours = Math.floor((uptime % 86400) / 3600);
            const minutes = Math.floor((uptime % 3600) / 60);
            const seconds = uptime % 60;
            const uptimeStr = days > 0 ? 
                `${days}d ${hours}h ${minutes}m ${seconds}s` : 
                `${hours}h ${minutes}m ${seconds}s`;
            
            // Update DOM elements
            document.getElementById('total-users').textContent = totalUsers;
            document.getElementById('total-jobs').textContent = totalJobs;
            document.getElementById('active-jobs').textContent = runningJobs;
            document.getElementById('dca-jobs').textContent = totalDcaJobs;
            document.getElementById('liq-jobs').textContent = totalLiqJobs;
            document.getElementById('total-positions').textContent = totalPositions;
            document.getElementById('uptime').textContent = uptimeStr;
        }
        
        function updateJobsTab(data) {
            const jobsContainer = document.getElementById('jobs-container');
            if (!jobsContainer) return;
            
            // Create a table of all jobs
            let html = `
                <table>
                    <thead>
                        <tr>
                            <th>User ID</th>
                            <th>Job ID</th>
                            <th>Name</th>
                            <th>Status</th>
                            <th>Progress</th>
                            <th>Amount</th>
                            <th>Created</th>
                        </tr>
                    </thead>
                    <tbody>
            `;
            
            // Collect all jobs from all users
            const allJobs = [];
            for (const userId in data.jobs_state) {
                for (const jobId in data.jobs_state[userId]) {
                    const job = data.jobs_state[userId][jobId];
                    allJobs.push({
                        userId,
                        jobId,
                        ...job
                    });
                }
            }
            
            // Sort jobs by timestamp (newest first)
            allJobs.sort((a, b) => {
                const timestampA = a.timestamp || '';
                const timestampB = b.timestamp || '';
                return timestampB.localeCompare(timestampA);
            });
            
            // Generate rows for each job
            allJobs.forEach(job => {
                const statusClass = getStatusColorClass(job.status);
                const completed = job.completed_steps || 0;
                const total = job.steps_total || 0;
                const progress = total > 0 ? Math.round((completed / total) * 100) : 0;
                
                html += `
                    <tr class="${statusClass} job-row" data-job-id="${job.jobId}" data-user-id="${job.userId}">
                        <td>${job.userId}</td>
                        <td>${job.jobId}</td>
                        <td>${job.name || 'N/A'}</td>
                        <td>${job.status || 'unknown'}</td>
                        <td>
                            ${total > 0 ? 
                                `<div class="w-full bg-gray-200 rounded-full h-2.5">
                                    <div class="bg-blue-600 h-2.5 rounded-full" style="width: ${progress}%"></div>
                                </div>
                                <div class="text-xs">${progress}% (${completed}/${total})</div>` : 
                                'N/A'}
                        </td>
                        <td>${formatAmount(job.amount)}</td>
                        <td>${formatTimestamp(job.timestamp)}</td>
                    </tr>
                `;
            });
            
            html += `
                    </tbody>
                </table>
            `;
            
            jobsContainer.innerHTML = html;
        }
        
        function updatePositionsTab(data) {
            const positionsContainer = document.getElementById('positions-container');
            if (!positionsContainer) return;
            
            // Create a table of all positions
            let html = '';
            
            if (Object.keys(data.positions_state).length === 0) {
                html = '<p>No positions data available</p>';
            } else {
                // Collect all positions from all users
                const allPositions = [];
                for (const userId in data.positions_state) {
                    for (const posKey in data.positions_state[userId]) {
                        const position = data.positions_state[userId][posKey];
                        allPositions.push({
                            userId,
                            ...position
                        });
                    }
                }
                
                // Sort positions by user ID
                allPositions.sort((a, b) => a.userId - b.userId);
                
                html = `
                    <table>
                    <thead>
                            <tr>
                                <th>User ID</th>
                                <th>Venue</th>
                                <th>Symbol</th>
                                <th>Side</th>
                                <th>Quantity</th>
                                <th>USDT</th>
                                <th>Entry Price</th>
                                <th>Mark Price</th>
                                <th>Leverage</th>
                                <th>PNL</th>
                                <th>Updated</th>
                        </tr>
                    </thead>
                    <tbody>
            `;
            
                // Generate rows for each position
                allPositions.forEach(pos => {
                    const pnlClass = getPnlColorClass(pos.unrealized_pnl);
                
                html += `
                        <tr>
                            <td>${pos.userId}</td>
                            <td>${pos.venue || 'N/A'}</td>
                            <td>${pos.symbol || 'N/A'}</td>
                            <td>${pos.side || 'N/A'}</td>
                            <td>${formatNumber(pos.qty)}</td>
                            <td>${formatNumber(pos.usdt_amt)}</td>
                            <td>${formatNumber(pos.entry_price)}</td>
                            <td>${formatNumber(pos.mark_price)}</td>
                            <td>${formatNumber(pos.leverage)}x</td>
                            <td class="${pnlClass}">${formatNumber(pos.unrealized_pnl)}</td>
                            <td>${formatTimestamp(pos.timestamp)}</td>
                    </tr>
                `;
            });
            
            html += `
                    </tbody>
                </table>
                `;
            }
            
            positionsContainer.innerHTML = html;
        }
        
        function updateRawDataTab(data) {
            const rawDataContainer = document.getElementById('raw-data-container');
            if (!rawDataContainer) return;
            
            // Create tabs for different types of data
            const html = `
                <div class="mb-4">
                    <div class="flex border-b">
                        <div class="px-4 py-2 cursor-pointer raw-data-tab active" data-target="raw-jobs">Jobs</div>
                        <div class="px-4 py-2 cursor-pointer raw-data-tab" data-target="raw-positions">Positions</div>
                        <div class="px-4 py-2 cursor-pointer raw-data-tab" data-target="raw-equity">Equity</div>
                        <div class="px-4 py-2 cursor-pointer raw-data-tab" data-target="raw-all">All Data</div>
                    </div>
                </div>
                
                <div id="raw-jobs" class="raw-data-content">
                    <h3 class="text-lg font-bold mb-2">Jobs Data</h3>
                    <pre class="raw-data">${JSON.stringify({
                        jobs_state: data.jobs_state,
                        dca_jobs: data.dca_jobs,
                        liq_jobs: data.liq_jobs,
                        job_to_user_map: data.job_to_user_map
                    }, null, 2)}</pre>
                </div>
                
                <div id="raw-positions" class="raw-data-content" style="display: none;">
                    <h3 class="text-lg font-bold mb-2">Positions Data</h3>
                    <pre class="raw-data">${JSON.stringify({
                        positions_state: data.positions_state
                    }, null, 2)}</pre>
                </div>
                
                <div id="raw-equity" class="raw-data-content" style="display: none;">
                    <h3 class="text-lg font-bold mb-2">Equity Data</h3>
                    <pre class="raw-data">${JSON.stringify({
                        equity_state: data.equity_state
                    }, null, 2)}</pre>
                </div>
                
                <div id="raw-all" class="raw-data-content" style="display: none;">
                    <h3 class="text-lg font-bold mb-2">All State Data</h3>
                    <pre class="raw-data">${JSON.stringify(data, null, 2)}</pre>
                </div>
            `;
            
            rawDataContainer.innerHTML = html;
            
            // Add event listeners to raw data tabs
            rawDataContainer.querySelectorAll('.raw-data-tab').forEach(tab => {
                tab.addEventListener('click', function() {
                    // Hide all content
                    rawDataContainer.querySelectorAll('.raw-data-content').forEach(content => {
                        content.style.display = 'none';
                    });
                    
                    // Remove active class from all tabs
                    rawDataContainer.querySelectorAll('.raw-data-tab').forEach(t => {
                        t.classList.remove('active');
                    });
                    
                    // Show selected content
                    const targetId = this.getAttribute('data-target');
                    document.getElementById(targetId).style.display = 'block';
                    
                    // Add active class to selected tab
                    this.classList.add('active');
                });
            });
        }
        
        function updateEquityTab(data) {
            const equityContainer = document.getElementById('equity-container');
            if (!equityContainer) return;
            
            // Create a table of all equity data
            let html = '';
            
            if (!data.equity_state || Object.keys(data.equity_state).length === 0) {
                html = '<p>No equity data available</p>';
            } else {
                // Collect all equity entries from all users
                const allEquity = [];
                for (const userId in data.equity_state) {
                    for (const venue in data.equity_state[userId]) {
                        const equity = data.equity_state[userId][venue];
                        allEquity.push({
                            userId,
                            venue,
                            ...equity
                        });
                    }
                }
                
                // Sort equity by user ID
                allEquity.sort((a, b) => a.userId - b.userId);
                
                html = `
                    <table>
                    <thead>
                            <tr>
                                <th>User ID</th>
                                <th>Venue</th>
                                <th>Total</th>
                                <th>Available</th>
                                <th>PNL</th>
                                <th>Updated</th>
                        </tr>
                    </thead>
                    <tbody>
            `;
            
                // Generate rows for each equity entry
                allEquity.forEach(eq => {
                    const pnlClass = getPnlColorClass(eq.unrealized_pnl);
                
                html += `
                        <tr>
                            <td>${eq.userId}</td>
                            <td>${eq.venue || 'N/A'}</td>
                            <td>${formatNumber(eq.wallet_balance)}</td>
                            <td>${formatNumber(eq.available_balance)}</td>
                            <td class="${pnlClass}">${formatNumber(eq.total_unrealized_pnl)}</td>
                            <td>${formatTimestamp(eq.timestamp)}</td>
                    </tr>
                `;
            });
            
            html += `
                    </tbody>
                </table>
                `;
            }
            
            equityContainer.innerHTML = html;
        }
        
        function addJobDetailListeners() {
            // Add click listeners to job rows
            document.querySelectorAll('.job-row').forEach(row => {
                row.addEventListener('click', function() {
                    const jobId = this.getAttribute('data-job-id');
                    const userId = this.getAttribute('data-user-id');
                    
                    if (jobId && userId && currentData) {
                        showJobDetails(userId, jobId);
                    }
                });
            });
            
            // Add click listener to close modal
            const closeButton = document.getElementById('closeJobDetailModal');
            if (closeButton) {
                closeButton.addEventListener('click', function() {
                    document.getElementById('jobDetailModal').style.display = 'none';
                });
            }
        }
        
        function showJobDetails(userId, jobId) {
            // Find the job in the current data
            if (!currentData || !currentData.jobs_state[userId] || !currentData.jobs_state[userId][jobId]) {
                console.error('Job not found:', userId, jobId);
                return;
            }
            
            const job = currentData.jobs_state[userId][jobId];
            
            // Populate modal with job details
            document.getElementById('jobDetailTitle').innerText = `Job #${jobId} (User ${userId})`;
            
            let html = `<pre>${JSON.stringify(job, null, 2)}</pre>`;
            document.getElementById('jobDetailContent').innerHTML = html;
            
            // Show the modal
            document.getElementById('jobDetailModal').style.display = 'block';
        }
        
        // Utility functions
        function getStatusColorClass(status) {
            if (!status) return '';
            
            switch(status) {
                case 'Running': return 'bg-green-100';
                case 'Paused': return 'bg-yellow-100';
                case 'Stopped': return 'bg-red-100';
                case 'Finished': return 'bg-blue-100';
                case 'Created': return 'bg-purple-100';
                default: return '';
            }
        }
        
        function getPnlColorClass(pnl) {
            if (pnl === undefined || pnl === null) return '';
            
            const numPnl = parseFloat(pnl);
            if (numPnl < 0) return 'negative-pnl';
            if (numPnl > 0) return 'positive-pnl';
            return '';
        }
        
        function formatAmount(amount) {
            if (!amount) return 'N/A';
            try {
                return `$${parseFloat(amount).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}`;
            } catch (e) {
                return amount;
            }
        }
        
        function formatNumber(num) {
            if (num === undefined || num === null) return 'N/A';
            try {
                return parseFloat(num).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 6});
            } catch (e) {
                return num;
            }
        }
        
        function formatTimestamp(timestamp) {
            if (!timestamp) return 'N/A';
            
            try {
                const date = new Date(timestamp);
                if (isNaN(date.getTime())) return timestamp;
                return date.toLocaleString();
            } catch (e) {
                return timestamp;
            }
        }
        
        // Switch tabs
        function showTab(tabId) {
            // Hide all tabs
            document.querySelectorAll('.tab-content').forEach(tab => {
                tab.classList.remove('active');
                tab.style.display = 'none';
            });
            document.querySelectorAll('.tab').forEach(tab => {
                tab.classList.remove('active');
            });
            
            // Show selected tab
            document.getElementById(tabId).style.display = 'block';
            document.getElementById(tabId).classList.add('active');
            document.querySelector(`.tab[onclick="showTab('${tabId}')"]`).classList.add('active');
        }
        
        // Initialize when the page loads
        document.addEventListener('DOMContentLoaded', function() {
            // Initial data load
            refreshData();
            
            // Set up auto-refresh
            setInterval(refreshData, 2000);
            
            // Initialize tabs
            document.querySelectorAll('.tab-content').forEach(tab => {
                if (!tab.classList.contains('active')) {
                    tab.style.display = 'none';
                }
            });
        });
    </script>
</head>
<body class="bg-gray-100">
    <div class="container mx-auto p-4">
        <header class="bg-blue-600 text-white p-4 mb-4 rounded shadow">
            <div class="flex justify-between items-center">
                <div>
                    <h1 class="text-2xl font-bold">TradeGuard Health Dashboard</h1>
                    <p>Last update: <span id="lastUpdate">-</span></p>
                </div>
                <div class="text-sm">
                    <div class="flex items-center">
                        <span class="inline-block w-2 h-2 bg-green-400 rounded-full mr-2 pulse"></span>
                        <span>Live data</span>
                    </div>
                </div>
            </div>
        </header>
        
        <!-- Summary Cards -->
            <div class="card">
                <h2>System Overview</h2>
            <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
                <div class="p-3 bg-white rounded shadow text-center">
                    <div class="text-2xl font-bold" id="total-users">-</div>
                    <div class="text-gray-500">Total Users</div>
                    </div>
                <div class="p-3 bg-white rounded shadow text-center">
                    <div class="text-2xl font-bold" id="total-jobs">-</div>
                    <div class="text-gray-500">Total Jobs</div>
                    </div>
                <div class="p-3 bg-white rounded shadow text-center">
                    <div class="text-2xl font-bold text-green-600" id="active-jobs">-</div>
                    <div class="text-gray-500">Active Jobs</div>
                    </div>
                <div class="p-3 bg-white rounded shadow text-center">
                    <div class="text-2xl font-bold" id="dca-jobs">-</div>
                    <div class="text-gray-500">DCA Jobs</div>
                    </div>
                <div class="p-3 bg-white rounded shadow text-center">
                    <div class="text-2xl font-bold" id="liq-jobs">-</div>
                    <div class="text-gray-500">LIQ Jobs</div>
                    </div>
                <div class="p-3 bg-white rounded shadow text-center">
                    <div class="text-2xl font-bold" id="total-positions">-</div>
                    <div class="text-gray-500">Positions</div>
                    </div>
                <div class="p-3 bg-white rounded shadow text-center md:col-span-2">
                    <div class="text-xl font-bold" id="uptime">-</div>
                    <div class="text-gray-500">Uptime</div>
                </div>
            </div>
        </div>
        
        <!-- Tabs -->
        <div class="mt-4">
            <div class="flex border-b">
                <div class="tab active" onclick="showTab('jobs-tab')">Jobs</div>
                <div class="tab" onclick="showTab('positions-tab')">Positions</div>
                <div class="tab" onclick="showTab('equity-tab')">Equity</div>
                <div class="tab" onclick="showTab('raw-data-tab')">Raw Data</div>
            </div>
            
            <!-- Jobs Tab -->
            <div id="jobs-tab" class="tab-content active">
                <div id="jobs-container">
                    <p>Loading jobs data...</p>
            </div>
        </div>
        
            <!-- Positions Tab -->
            <div id="positions-tab" class="tab-content">
                <div id="positions-container">
                    <p>Loading positions data...</p>
            </div>
        </div>
        
            <!-- Equity Tab -->
            <div id="equity-tab" class="tab-content">
                <div id="equity-container">
                    <p>Loading equity data...</p>
                </div>
            </div>
            
            <!-- Raw Data Tab -->
            <div id="raw-data-tab" class="tab-content">
                <div id="raw-data-container">
                    <p>Loading raw data...</p>
                </div>
            </div>
        </div>
    </div>
    
    <!-- Job Detail Modal -->
    <div id="jobDetailModal" class="modal">
        <div class="modal-content">
            <div class="flex justify-between items-center mb-4">
                <h2 id="jobDetailTitle" class="text-xl font-bold">Job Details</h2>
                <span id="closeJobDetailModal" class="close">&times;</span>
            </div>
            <div id="jobDetailContent" class="mt-4">
                <!-- Job details will be inserted here -->
            </div>
        </div>
    </div>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    """Return the dashboard HTML page."""
    return DASHBOARD_HTML

@app.get("/api/ping")
async def ping():
    return "Hello"

@app.get("/api/state")
async def get_state():
    """Return the current state as JSON."""
    return dashboard_state

class WebDashboard:
    """Web-based dashboard for displaying application state."""
    
    def __init__(self):
        """Initialize the dashboard."""
        self.server = None
        self.server_thread = None
        self.running = False
    
    def set_state_data(self, jobs_state, dca_jobs, liq_jobs, job_to_user_map, positions_state=None, equity_state=None):
        """
        Update the dashboard with current state data.
        
        Args:
            jobs_state: Dictionary mapping user_id to dictionary of job_id -> job_data
            dca_jobs: Dictionary mapping user_id to dictionary of job_id -> job_data for DCA jobs
            liq_jobs: Dictionary mapping user_id to dictionary of job_id -> job_data for LIQ jobs
            job_to_user_map: Mapping of job_id to user_id
            positions_state: Dictionary mapping user_id to dictionary of position_key -> position_data
            equity_state: Dictionary mapping user_id to dictionary of venue -> equity_data
        """
        global dashboard_state

        dashboard_state["jobs_state"] = self._make_serializable_copy(jobs_state)
        dashboard_state["dca_jobs"] = self._make_serializable_copy(dca_jobs)
        dashboard_state["liq_jobs"] = self._make_serializable_copy(liq_jobs)
        dashboard_state["job_to_user_map"] = dict(job_to_user_map)
        
        if positions_state is not None:
            dashboard_state["positions_state"] = self._make_serializable_copy(positions_state)
            self._format_timestamps(dashboard_state["positions_state"])
            
        if equity_state is not None:
            dashboard_state["equity_state"] = self._make_serializable_copy(equity_state)
            self._format_timestamps(dashboard_state["equity_state"])
            
        dashboard_state["last_update"] = time.time()

        self._format_timestamps(dashboard_state["jobs_state"])
        self._format_timestamps(dashboard_state["dca_jobs"])
        self._format_timestamps(dashboard_state["liq_jobs"])
    
    def _format_timestamps(self, state_dict):
        """Format all timestamps in the state dictionary for better display."""
        for user_id, items in state_dict.items():
            for item_id, item in items.items():
                if isinstance(item, dict) and 'timestamp' in item and item['timestamp']:
                    try:
                        # Ensure it's ISO format
                        dt = DateTimeUtils.parse_timestamp(item['timestamp'])
                        if dt:
                            item['timestamp'] = dt.isoformat()
                    except Exception:
                        # Keep original if parsing fails
                        pass
    
    def _make_serializable_copy(self, nested_dict):
        """Create a serializable copy of the state dictionaries."""
        result = {}
        for user_id, items in nested_dict.items():
            result[user_id] = {}
            for item_id, item in items.items():
                # Ensure all values are JSON serializable
                if isinstance(item, dict):
                    result[user_id][item_id] = dict(item)
                else:
                    # Handle non-dict items (should be rare but possible)
                    result[user_id][item_id] = item
        return result
    
    def start_server(self):
        """Start the web server in the current thread."""
        logger.info(f"Starting web dashboard at http://{Config.DASHBOARD_HOST}:{Config.DASHBOARD_PORT}")
        uvicorn.run(app, host=Config.DASHBOARD_HOST, port=Config.DASHBOARD_PORT)
    
    def start_server_in_thread(self):
        """Start the web server in a background thread."""
        logger.info(f"Starting web dashboard in background thread at http://{Config.DASHBOARD_HOST}:{Config.DASHBOARD_PORT}")
        self.server_thread = threading.Thread(target=self.start_server, daemon=True)
        self.server_thread.start()
        return self.server_thread

    def stop_server(self):
        """Stop the web server."""
        if self.server:
            logger.info("Stopping web dashboard server...")
            self.server.should_exit = True
            if self.server_thread:
                self.server_thread.join(timeout=5)
                logger.info("Web dashboard server stopped")
