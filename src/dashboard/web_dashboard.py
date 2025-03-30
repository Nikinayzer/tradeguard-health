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
    </style>
    <script>
        // Store current state data
        let currentData = null;
        let selectedUserId = null;
        
        // Auto-refresh data every 2 seconds
        function refreshData() {
            fetch('/api/state')
                .then(response => response.json())
                .then(data => {
                    // Store the data for modal usage
                    currentData = data;
                    
                    document.getElementById('summaryData').innerHTML = renderSummary(data);
                    document.getElementById('statusData').innerHTML = renderStatusTable(data);
                    document.getElementById('jobsData').innerHTML = renderRecentJobs(data);
                    document.getElementById('usersData').innerHTML = renderActiveUsers(data);
                    
                    // If a user is selected, update user context view
                    if (selectedUserId && document.getElementById('userContextData')) {
                        document.getElementById('userContextData').innerHTML = renderUserContext(selectedUserId);
                        document.getElementById('userContextTitle').innerText = `User ${selectedUserId} Context`;
                    }
                    
                    // Update last refresh time
                    const now = new Date();
                    document.getElementById('lastUpdate').innerText = now.toLocaleTimeString();
                    
                    // Add click listeners for job details
                    addJobDetailListeners();
                    
                    // Add click listeners for user selection
                    addUserSelectionListeners();
                })
                .catch(error => console.error('Error fetching data:', error));
        }
        
        function addUserSelectionListeners() {
            // Add click listeners to user rows
            document.querySelectorAll('.user-row').forEach(row => {
                row.addEventListener('click', function() {
                    const userId = this.getAttribute('data-user-id');
                    
                    if (userId && currentData) {
                        selectedUserId = userId;
                        showUserContext(userId);
                    }
                });
            });
        }
        
        function showUserContext(userId) {
            if (!currentData || !currentData.jobs_state[userId]) {
                console.error('User not found:', userId);
                return;
            }
            
            // Show the user context section
            const userContextSection = document.getElementById('userContextSection');
            if (userContextSection) {
                userContextSection.style.display = 'block';
                document.getElementById('userContextTitle').innerText = `User ${userId} Context`;
                document.getElementById('userContextData').innerHTML = renderUserContext(userId);
                
                // Scroll to the section
                userContextSection.scrollIntoView({ behavior: 'smooth' });
            }
        }
        
        function renderUserContext(userId) {
            if (!currentData || !currentData.jobs_state[userId]) {
                return '<div class="p-4 bg-red-100 text-red-700">User data not found</div>';
            }
            
            const jobs = currentData.jobs_state[userId];
            
            // Sort jobs by timestamp (newest first)
            const sortedJobs = Object.entries(jobs)
                .map(([jobId, job]) => ({ jobId, ...job }))
                .sort((a, b) => (b.timestamp || '').localeCompare(a.timestamp || ''));
            
            // Calculate user statistics
            const totalJobs = sortedJobs.length;
            const runningJobs = sortedJobs.filter(job => job.status === 'Running').length;
            const pausedJobs = sortedJobs.filter(job => job.status === 'Paused').length;
            const stoppedJobs = sortedJobs.filter(job => job.status === 'Stopped').length;
            const finishedJobs = sortedJobs.filter(job => job.status === 'Finished').length;
            const dcaJobs = sortedJobs.filter(job => job.name === 'dca').length;
            const liqJobs = sortedJobs.filter(job => job.name === 'liq').length;
            
            // Calculate total amount and total discount
            let totalAmount = 0;
            sortedJobs.forEach(job => {
                if (job.amount) {
                    totalAmount += parseFloat(job.amount);
                }
            });
            
            // Format total amount
            const formattedTotalAmount = `$${totalAmount.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}`;
            
            // Status color mapping
            const statusColors = {
                'Created': 'bg-blue-100',
                'Running': 'bg-green-100',
                'Paused': 'bg-yellow-100',
                'Stopped': 'bg-red-100',
                'Finished': 'bg-green-200',
                'unknown': 'bg-gray-100'
            };
            
            return `
                <div class="space-y-6">
                    <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
                        <div class="p-4 bg-white rounded shadow text-center">
                            <div class="text-3xl font-bold">${totalJobs}</div>
                            <div class="text-gray-500">Total Jobs</div>
                        </div>
                        <div class="p-4 bg-white rounded shadow text-center">
                            <div class="text-3xl font-bold text-green-600">${runningJobs}</div>
                            <div class="text-gray-500">Running</div>
                        </div>
                        <div class="p-4 bg-white rounded shadow text-center">
                            <div class="text-3xl font-bold">${formattedTotalAmount}</div>
                            <div class="text-gray-500">Total Amount</div>
                        </div>
                        <div class="p-4 bg-white rounded shadow text-center">
                            <div class="text-3xl font-bold">${dcaJobs}/${liqJobs}</div>
                            <div class="text-gray-500">DCA/LIQ</div>
                        </div>
                    </div>
                    
                    <div class="bg-white rounded shadow p-4">
                        <h3 class="text-lg font-bold mb-2">All Jobs</h3>
                        <table class="min-w-full">
                            <thead>
                                <tr class="bg-gray-200">
                                    <th class="px-4 py-2 text-left">Job ID</th>
                                    <th class="px-4 py-2 text-left">Name</th>
                                    <th class="px-4 py-2 text-left">Status</th>
                                    <th class="px-4 py-2 text-left">Progress</th>
                                    <th class="px-4 py-2 text-right">Amount</th>
                                    <th class="px-4 py-2 text-right">Discount</th>
                                    <th class="px-4 py-2 text-left">Timestamp</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${sortedJobs.map(job => {
                                    const completed = job.completed_steps || 0;
                                    const total = job.steps_total || 0;
                                    
                                    // Calculate progress percentage
                                    const progressPct = total > 0 ? Math.round((completed / total) * 100) : 0;
                                    
                                    // Format progress bar
                                    const progressBar = total > 0 ? 
                                        `<div class="w-full bg-gray-200 rounded-full h-2.5">
                                            <div class="bg-blue-600 h-2.5 rounded-full" style="width: ${progressPct}%"></div>
                                        </div>
                                        <div class="text-xs text-right">${progressPct}% (${completed}/${total})</div>` : 
                                        'N/A';
                                    
                                    // Format amount and discount
                                    const amount = job.amount ? `$${parseFloat(job.amount).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}` : 'N/A';
                                    const discount = job.discount_pct ? `${parseFloat(job.discount_pct).toFixed(2)}%` : 'N/A';
                                    
                                    const rowClass = statusColors[job.status] || '';
                                    
                                    return `
                                        <tr class="${rowClass} hover:bg-opacity-80 cursor-pointer job-row" data-job-id="${job.jobId}" data-user-id="${userId}">
                                            <td class="border px-4 py-2">${job.jobId}</td>
                                            <td class="border px-4 py-2">${job.name || ''}</td>
                                            <td class="border px-4 py-2">${job.status || 'unknown'}</td>
                                            <td class="border px-4 py-2 w-32">${progressBar}</td>
                                            <td class="border px-4 py-2 text-right">${amount}</td>
                                            <td class="border px-4 py-2 text-right">${discount}</td>
                                            <td class="border px-4 py-2">${formatTimestamp(job.timestamp)}</td>
                                        </tr>
                                    `;
                                }).join('')}
                            </tbody>
                        </table>
                    </div>
                    
                    <div class="bg-white rounded shadow p-4">
                        <h3 class="text-lg font-bold mb-2">Status Distribution</h3>
                        <div class="w-full bg-gray-200 h-8 rounded-full overflow-hidden flex">
                            <div class="bg-green-500 h-full text-xs text-white flex items-center justify-center" 
                                style="width: ${totalJobs > 0 ? (runningJobs / totalJobs * 100) : 0}%">
                                ${runningJobs > 0 ? `${Math.round(runningJobs / totalJobs * 100)}%` : ''}
                            </div>
                            <div class="bg-yellow-500 h-full text-xs text-white flex items-center justify-center" 
                                style="width: ${totalJobs > 0 ? (pausedJobs / totalJobs * 100) : 0}%">
                                ${pausedJobs > 0 ? `${Math.round(pausedJobs / totalJobs * 100)}%` : ''}
                            </div>
                            <div class="bg-red-500 h-full text-xs text-white flex items-center justify-center" 
                                style="width: ${totalJobs > 0 ? (stoppedJobs / totalJobs * 100) : 0}%">
                                ${stoppedJobs > 0 ? `${Math.round(stoppedJobs / totalJobs * 100)}%` : ''}
                            </div>
                            <div class="bg-blue-500 h-full text-xs text-white flex items-center justify-center" 
                                style="width: ${totalJobs > 0 ? (finishedJobs / totalJobs * 100) : 0}%">
                                ${finishedJobs > 0 ? `${Math.round(finishedJobs / totalJobs * 100)}%` : ''}
                            </div>
                        </div>
                        <div class="flex justify-between text-xs mt-1">
                            <div><span class="inline-block w-2 h-2 bg-green-500 rounded-full"></span> Running (${runningJobs})</div>
                            <div><span class="inline-block w-2 h-2 bg-yellow-500 rounded-full"></span> Paused (${pausedJobs})</div>
                            <div><span class="inline-block w-2 h-2 bg-red-500 rounded-full"></span> Stopped (${stoppedJobs})</div>
                            <div><span class="inline-block w-2 h-2 bg-blue-500 rounded-full"></span> Finished (${finishedJobs})</div>
                        </div>
                    </div>
                </div>
            `;
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
        }
        
        function showJobDetails(userId, jobId) {
            // Find the job in the current data
            if (!currentData || !currentData.jobs_state[userId] || !currentData.jobs_state[userId][jobId]) {
                console.error('Job not found:', userId, jobId);
                return;
            }
            
            const job = currentData.jobs_state[userId][jobId];
            
            // Populate modal with job details
            document.getElementById('jobDetailTitle').innerText = `Job #${jobId}`;
            document.getElementById('jobDetailContent').innerHTML = renderJobDetailContent(job, userId);
            
            // Show the modal
            document.getElementById('jobDetailModal').style.display = 'block';
        }
        
        function renderJobDetailContent(job, userId) {
            // Format job details into HTML
            const completed = job.completed_steps || 0;
            const total = job.steps_total || 0;
            const progressPct = total > 0 ? Math.round((completed / total) * 100) : 0;
            
            // Format job properties for display
            const props = [];
            for (const [key, value] of Object.entries(job)) {
                // Skip displaying large arrays or objects inline
                if (Array.isArray(value) && value.length > 5) {
                    props.push(`<tr>
                        <td class="font-semibold p-2 border-r">${key}</td>
                        <td class="p-2">[Array with ${value.length} items]</td>
                    </tr>`);
                } else if (typeof value === 'object' && value !== null) {
                    props.push(`<tr>
                        <td class="font-semibold p-2 border-r">${key}</td>
                        <td class="p-2"><pre class="text-xs overflow-auto">${JSON.stringify(value, null, 2)}</pre></td>
                    </tr>`);
                } else {
                    props.push(`<tr>
                        <td class="font-semibold p-2 border-r">${key}</td>
                        <td class="p-2">${value}</td>
                    </tr>`);
                }
            }
            
            return `
                <div class="grid grid-cols-1 gap-4">
                    <div class="bg-gray-100 p-4 rounded">
                        <div class="grid grid-cols-2 gap-2">
                            <div>User ID:</div><div class="font-bold">${userId}</div>
                            <div>Job ID:</div><div class="font-bold">${job.job_id}</div>
                            <div>Job Type:</div><div class="font-bold">${job.name || 'Unknown'}</div>
                            <div>Status:</div><div class="font-bold">${job.status || 'Unknown'}</div>
                        </div>
                    </div>
                    
                    <div>
                        <h3 class="text-lg font-bold mb-2">Progress</h3>
                        ${total > 0 ? `
                            <div class="w-full bg-gray-200 rounded-full h-2.5 mb-1">
                                <div class="bg-blue-600 h-2.5 rounded-full" style="width: ${progressPct}%"></div>
                            </div>
                            <div class="text-xs text-right">${progressPct}% (${completed}/${total} steps)</div>
                        ` : '<div>No progress data available</div>'}
                    </div>
                    
                    <div>
                        <h3 class="text-lg font-bold mb-2">All Properties</h3>
                        <div class="overflow-x-auto">
                            <table class="min-w-full border">
                                <tbody>
                                    ${props.join('')}
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            `;
        }
        
        function closeJobDetailModal() {
            document.getElementById('jobDetailModal').style.display = 'none';
        }
        
        function renderSummary(data) {
            const totalJobs = Object.values(data.jobs_state).reduce((sum, jobs) => sum + Object.keys(jobs).length, 0);
            const totalDcaJobs = Object.values(data.dca_jobs).reduce((sum, jobs) => sum + Object.keys(jobs).length, 0);
            const totalLiqJobs = Object.values(data.liq_jobs).reduce((sum, jobs) => sum + Object.keys(jobs).length, 0);
            const userCount = Object.keys(data.jobs_state).length;
            const jobMapCount = Object.keys(data.job_to_user_map).length;
            
            // Calculate running jobs
            const runningJobs = Object.values(data.jobs_state).reduce((sum, jobs) => {
                return sum + Object.values(jobs).filter(j => j.status === 'Running').length;
            }, 0);
            
            // Calculate total job steps and completion percentage
            let totalSteps = 0;
            let completedSteps = 0;
            Object.values(data.jobs_state).forEach(jobs => {
                Object.values(jobs).forEach(job => {
                    if (job.steps_total) {
                        totalSteps += job.steps_total;
                        completedSteps += job.completed_steps || 0;
                    }
                });
            });
            
            
            // Calculate uptime
            const uptime = Math.floor((Date.now() / 1000) - data.startup_time);
            const days = Math.floor(uptime / 86400);
            const hours = Math.floor((uptime % 86400) / 3600);
            const minutes = Math.floor((uptime % 3600) / 60);
            const seconds = uptime % 60;
            const uptimeStr = days > 0 ? 
                `${days}d ${hours}h ${minutes}m ${seconds}s` : 
                `${hours}h ${minutes}m ${seconds}s`;
            
            return `
                <div class="grid grid-cols-2 gap-2">
                    <div>Total Jobs:</div><div class="font-bold">${totalJobs}</div>
                    <div>Running Jobs:</div><div class="font-bold text-green-600">${runningJobs}</div>
                    <div>DCA Jobs:</div><div class="font-bold">${totalDcaJobs}</div>
                    <div>LIQ Jobs:</div><div class="font-bold">${totalLiqJobs}</div>
                    <div>Users:</div><div class="font-bold">${userCount}</div>
                    <div>Job-User Mappings:</div><div class="font-bold">${jobMapCount}</div>
                    <div>Uptime:</div><div class="font-bold">${uptimeStr}</div>
                </div>
            `;
        }
        
        function renderStatusTable(data) {
            // Count jobs by status
            const statusCounts = {};
            
            Object.values(data.jobs_state).forEach(jobs => {
                Object.values(jobs).forEach(job => {
                    const status = job.status || 'unknown';
                    statusCounts[status] = (statusCounts[status] || 0) + 1;
                });
            });
            
            // Calculate total for percentages
            const total = Object.values(statusCounts).reduce((sum, count) => sum + count, 0);
            
            // Generate HTML
            let html = `
                <table class="min-w-full">
                    <thead>
                        <tr class="bg-gray-200">
                            <th class="px-4 py-2 text-left">Status</th>
                            <th class="px-4 py-2 text-right">Count</th>
                            <th class="px-4 py-2 text-right">Percentage</th>
                        </tr>
                    </thead>
                    <tbody>
            `;
            
            // Status color mapping
            const statusColors = {
                'Created': 'bg-blue-100',
                'Running': 'bg-green-100',
                'Paused': 'bg-yellow-100',
                'Stopped': 'bg-red-100',
                'Finished': 'bg-green-200',
                'unknown': 'bg-gray-100'
            };
            
            Object.entries(statusCounts).sort().forEach(([status, count]) => {
                const percentage = total > 0 ? ((count / total) * 100).toFixed(1) : '0.0';
                const colorClass = statusColors[status] || 'bg-white';
                html += `
                    <tr class="${colorClass}">
                        <td class="border px-4 py-2">${status}</td>
                        <td class="border px-4 py-2 text-right">${count}</td>
                        <td class="border px-4 py-2 text-right">${percentage}%</td>
                    </tr>
                `;
            });
            
            if (total > 0) {
                html += `
                    <tr class="bg-gray-200 font-bold">
                        <td class="border px-4 py-2">Total</td>
                        <td class="border px-4 py-2 text-right">${total}</td>
                        <td class="border px-4 py-2 text-right">100%</td>
                    </tr>
                `;
            }
            
            html += `
                    </tbody>
                </table>
            `;
            
            return html;
        }
        
        function formatTimestamp(timestamp) {
            if (!timestamp) return 'N/A';
            
            try {
                const date = new Date(timestamp);
                if (isNaN(date.getTime())) return timestamp;
                
                // Check if it's today
                const today = new Date();
                if (date.toDateString() === today.toDateString()) {
                    return `Today at ${date.toLocaleTimeString()}`;
                }
                
                // Check if it's yesterday
                const yesterday = new Date();
                yesterday.setDate(yesterday.getDate() - 1);
                if (date.toDateString() === yesterday.toDateString()) {
                    return `Yesterday at ${date.toLocaleTimeString()}`;
                }
                
                // Otherwise show full date
                return date.toLocaleString();
            } catch (e) {
                return timestamp;
            }
        }
        
        function renderRecentJobs(data) {
            // Collect all jobs with timestamps
            const allJobs = [];
            
            Object.entries(data.jobs_state).forEach(([user_id, jobs]) => {
                Object.entries(jobs).forEach(([job_id, job]) => {
                    allJobs.push({
                        timestamp: job.timestamp || '',
                        user_id: user_id,
                        job_id: job_id,
                        job: job
                    });
                });
            });
            
            // Sort by timestamp (newest first) and take top 10
            allJobs.sort((a, b) => b.timestamp.localeCompare(a.timestamp));
            const recentJobs = allJobs.slice(0, 10);
            
            // Generate HTML
            let html = `
                <table class="min-w-full">
                    <thead>
                        <tr class="bg-gray-200">
                            <th class="px-4 py-2 text-left">Job ID</th>
                            <th class="px-4 py-2 text-left">User ID</th>
                            <th class="px-4 py-2 text-left">Name</th>
                            <th class="px-4 py-2 text-left">Status</th>
                            <th class="px-4 py-2 text-left">Progress</th>
                            <th class="px-4 py-2 text-right">Amount</th>
                            <th class="px-4 py-2 text-right">Discount</th>
                            <th class="px-4 py-2 text-left">Timestamp</th>
                        </tr>
                    </thead>
                    <tbody>
            `;
            
            // Status color mapping
            const statusColors = {
                'Created': 'bg-blue-100',
                'Running': 'bg-green-100',
                'Paused': 'bg-yellow-100',
                'Stopped': 'bg-red-100',
                'Finished': 'bg-green-200',
                'unknown': 'bg-gray-100'
            };
            
            recentJobs.forEach(({user_id, job_id, job}) => {
                const completed = job.completed_steps || 0;
                const total = job.steps_total || 0;
                
                // Calculate progress percentage
                const progressPct = total > 0 ? Math.round((completed / total) * 100) : 0;
                
                // Format progress bar
                const progressBar = total > 0 ? 
                    `<div class="w-full bg-gray-200 rounded-full h-2.5">
                        <div class="bg-blue-600 h-2.5 rounded-full" style="width: ${progressPct}%"></div>
                    </div>
                    <div class="text-xs text-right">${progressPct}% (${completed}/${total})</div>` : 
                    'N/A';
                
                // Format amount and discount
                const amount = job.amount ? `$${parseFloat(job.amount).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}` : 'N/A';
                const discount = job.discount_pct ? `${parseFloat(job.discount_pct).toFixed(2)}%` : 'N/A';
                
                const rowClass = statusColors[job.status] || '';
                
                html += `
                    <tr class="${rowClass} hover:bg-opacity-80 cursor-pointer job-row" data-job-id="${job_id}" data-user-id="${user_id}">
                        <td class="border px-4 py-2">${job_id}</td>
                        <td class="border px-4 py-2">${user_id}</td>
                        <td class="border px-4 py-2">${job.name || ''}</td>
                        <td class="border px-4 py-2">${job.status || 'unknown'}</td>
                        <td class="border px-4 py-2 w-32">${progressBar}</td>
                        <td class="border px-4 py-2 text-right">${amount}</td>
                        <td class="border px-4 py-2 text-right">${discount}</td>
                        <td class="border px-4 py-2">${formatTimestamp(job.timestamp)}</td>
                    </tr>
                `;
            });
            
            html += `
                    </tbody>
                </table>
                <div class="text-sm text-gray-500 mt-2">Click on any job row to view detailed information</div>
            `;
            
            return html;
        }
        
        function renderActiveUsers(data) {
            // Count jobs by user
            const userStats = {};
            
            Object.entries(data.jobs_state).forEach(([user_id, jobs]) => {
                const activeJobs = Object.values(jobs).filter(
                    job => !['Finished', 'Stopped'].includes(job.status)
                ).length;
                
                const runningJobs = Object.values(jobs).filter(
                    job => job.status === 'Running'
                ).length;
                
                const dcaCount = data.dca_jobs[user_id] ? Object.keys(data.dca_jobs[user_id]).length : 0;
                const liqCount = data.liq_jobs[user_id] ? Object.keys(data.liq_jobs[user_id]).length : 0;
                
                // Calculate completion percentage for this user
                let totalSteps = 0;
                let completedSteps = 0;
                
                Object.values(jobs).forEach(job => {
                    if (job.steps_total) {
                        totalSteps += job.steps_total;
                        completedSteps += job.completed_steps || 0;
                    }
                });
                
                userStats[user_id] = {
                    total: Object.keys(jobs).length,
                    active: activeJobs,
                    running: runningJobs,
                    dca: dcaCount,
                    liq: liqCount,
                    steps_total: totalSteps,
                    steps_completed: completedSteps
                };
            });
            
            // Sort by active jobs, descending
            const sortedUsers = Object.entries(userStats)
                .sort((a, b) => b[1].active - a[1].active)
                .slice(0, 10);  // Show top 10 users
            
            // Generate HTML
            let html = `
                <table class="min-w-full">
                    <thead>
                        <tr class="bg-gray-200">
                            <th class="px-4 py-2 text-left">User ID</th>
                            <th class="px-4 py-2 text-right">Active</th>
                            <th class="px-4 py-2 text-right">Running</th>
                            <th class="px-4 py-2 text-right">Total</th>
                            <th class="px-4 py-2 text-right">DCA</th>
                            <th class="px-4 py-2 text-right">LIQ</th>
                            <th class="px-4 py-2 text-left">Completion</th>
                        </tr>
                    </thead>
                    <tbody>
            `;
            
            sortedUsers.forEach(([user_id, stats]) => {
                // Calculate completion percentage
                const completionPct = stats.steps_total > 0 ? 
                    Math.round((stats.steps_completed / stats.steps_total) * 100) : 0;
                
                // Format progress bar
                const progressBar = stats.steps_total > 0 ? 
                    `<div class="w-full bg-gray-200 rounded-full h-2.5">
                        <div class="bg-blue-600 h-2.5 rounded-full" style="width: ${completionPct}%"></div>
                    </div>
                    <div class="text-xs text-right">${completionPct}%</div>` : 
                    'N/A';
                
                html += `
                    <tr class="hover:bg-gray-100 cursor-pointer user-row" data-user-id="${user_id}">
                        <td class="border px-4 py-2">${user_id}</td>
                        <td class="border px-4 py-2 text-right">${stats.active}</td>
                        <td class="border px-4 py-2 text-right text-green-600">${stats.running}</td>
                        <td class="border px-4 py-2 text-right">${stats.total}</td>
                        <td class="border px-4 py-2 text-right">${stats.dca}</td>
                        <td class="border px-4 py-2 text-right">${stats.liq}</td>
                        <td class="border px-4 py-2 w-32">${progressBar}</td>
                    </tr>
                `;
            });
            
            html += `
                    </tbody>
                </table>
                <div class="text-sm text-gray-500 mt-2">Click on any user row to view all jobs for that user</div>
            `;
            
            return html;
        }
        
        // Initialize when the page loads
        document.addEventListener('DOMContentLoaded', function() {
            refreshData();
            setInterval(refreshData, 2000);
            
            // Setup modal close button
            document.getElementById('closeJobDetailModal').addEventListener('click', closeJobDetailModal);
            
            // Close modal if user clicks outside
            window.addEventListener('click', function(event) {
                const modal = document.getElementById('jobDetailModal');
                if (event.target === modal) {
                    closeJobDetailModal();
                }
            });
        });
    </script>
</head>
<body class="bg-gray-50">
    <div class="container mx-auto p-4">
        <header class="bg-blue-600 text-white p-4 mb-6 rounded shadow">
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
        
        <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div class="bg-white p-4 rounded shadow">
                <h2 class="text-xl font-bold mb-4">Summary</h2>
                <div id="summaryData">Loading...</div>
            </div>
            
            <div class="bg-white p-4 rounded shadow">
                <h2 class="text-xl font-bold mb-4">Jobs by Status</h2>
                <div id="statusData">Loading...</div>
            </div>
            
            <div class="bg-white p-4 rounded shadow col-span-1 md:col-span-2">
                <h2 class="text-xl font-bold mb-4">Recent Jobs</h2>
                <div id="jobsData">Loading...</div>
            </div>
            
            <div class="bg-white p-4 rounded shadow col-span-1 md:col-span-2">
                <h2 class="text-xl font-bold mb-4">Users with Active Jobs</h2>
                <div id="usersData">Loading...</div>
            </div>
        </div>
        
        <!-- User Context Section (initially hidden) -->
        <div id="userContextSection" class="mt-6" style="display: none;">
            <div class="bg-white p-4 rounded shadow">
                <h2 id="userContextTitle" class="text-xl font-bold mb-4">User Context</h2>
                <div id="userContextData">Loading...</div>
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
    
    def set_state_data(self, jobs_state, dca_jobs, liq_jobs, job_to_user_map):
        """
        Update the dashboard with current state data.
        
        Args:
            jobs_state: Dictionary mapping user_id to dictionary of job_id -> job_data
            dca_jobs: Dictionary mapping user_id to dictionary of job_id -> job_data for DCA jobs
            liq_jobs: Dictionary mapping user_id to dictionary of job_id -> job_data for LIQ jobs
            job_to_user_map: Mapping of job_id to user_id
        """
        global dashboard_state

        dashboard_state["jobs_state"] = self._make_serializable_copy(jobs_state)
        dashboard_state["dca_jobs"] = self._make_serializable_copy(dca_jobs)
        dashboard_state["liq_jobs"] = self._make_serializable_copy(liq_jobs)
        dashboard_state["job_to_user_map"] = dict(job_to_user_map)
        dashboard_state["last_update"] = time.time()

        self._format_timestamps(dashboard_state["jobs_state"])
        self._format_timestamps(dashboard_state["dca_jobs"])
        self._format_timestamps(dashboard_state["liq_jobs"])
    
    def _format_timestamps(self, state_dict):
        """Format all timestamps in the state dictionary for better display."""
        for user_id, jobs in state_dict.items():
            for job_id, job in jobs.items():
                if 'timestamp' in job and job['timestamp']:
                    try:
                        # Ensure it's ISO format
                        dt = DateTimeUtils.parse_timestamp(job['timestamp'])
                        if dt:
                            job['timestamp'] = dt.isoformat()
                    except Exception:
                        # Keep original if parsing fails
                        pass
    
    def _make_serializable_copy(self, nested_dict):
        """Create a serializable copy of the state dictionaries."""
        result = {}
        for user_id, jobs in nested_dict.items():
            result[user_id] = {}
            for job_id, job in jobs.items():
                # Ensure all values are JSON serializable
                result[user_id][job_id] = dict(job)
        return result
    
    def start_server(self):
        """Start the FastAPI server in the current thread."""
        logger.info(f"Starting web dashboard at http://localhost:{Config.DASHBOARD_PORT}")

        uvicorn.run(
            app, 
            host=Config.DASHBOARD_HOST, 
            port=Config.DASHBOARD_PORT,
            log_level="warning",
            access_log=False
        )
    
    def start_server_in_thread(self):
        """Start the FastAPI server in a background thread."""
        self.running = True
        self.server_thread = threading.Thread(
            target=self.start_server,
            daemon=True
        )
        self.server_thread.start()
        logger.info(f"Web dashboard started in background thread at http://{Config.DASHBOARD_HOST}:{Config.DASHBOARD_PORT}")
        return self.server_thread
