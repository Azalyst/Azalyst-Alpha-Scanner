# Complete GitHub Setup Instructions for Azalyst Whale Tracking Agent

## OBJECTIVE
Push the NIM Qwen Agent with Azalyst whale tracking to GitHub and configure all necessary GitHub Actions, secrets, and workflows.

---

## PREREQUISITES

**Required API Keys:**
1. **NIM_API_KEY** - Get from https://build.nvidia.com (REQUIRED)
2. **HELIUS_API_KEY** - Get from https://Azalyst.so (Optional but recommended)

**Repository:**
- GitHub repository URL: `<YOUR_REPO_URL>`
- Repository must exist (create if needed)

---

## STEP 1: FILE STRUCTURE SETUP

Create the following directory structure in the repository:

```
repo/
├── .github/
│   └── workflows/
│       ├── agent.yml              # Main agent workflow
│       └── whale_tracking.yml     # Whale tracking workflow
├── agent/
│   ├── agent.py
│   ├── tools.py
│   └── Azalyst_tracker.py
├── AGENTS.md
├── Azalyst_USAGE.md
├── README.md
├── example_whale_tracking.py
└── requirements.txt
```

---

## STEP 2: COPY FILES FROM D:\files

Execute these file operations:

### A. Update existing files in D:\files:

```bash
# Replace these files with updated versions
cp D:\files\outputs\tools_updated.py D:\files\agent\tools.py
cp D:\files\outputs\requirements_updated.txt D:\files\requirements.txt
cp D:\files\outputs\AGENTS_updated.md D:\files\AGENTS.md
```

### B. Add new files to D:\files:

```bash
# Copy new files to appropriate locations
cp D:\files\outputs\Azalyst_tracker.py D:\files\agent\Azalyst_tracker.py
cp D:\files\outputs\Azalyst_USAGE.md D:\files\Azalyst_USAGE.md
cp D:\files\outputs\whale_tracking_workflow.yml D:\files\.github\workflows\whale_tracking.yml
cp D:\files\outputs\example_whale_tracking.py D:\files\example_whale_tracking.py
```

### C. Create/Update README.md:

```bash
# Use the new comprehensive README
cp D:\files\outputs\README_Azalyst.md D:\files\README.md
```

---

## STEP 3: CREATE MAIN AGENT WORKFLOW

Create file: `.github/workflows/agent.yml`

```yaml
name: NIM Qwen Agent

on:
  workflow_dispatch:
    inputs:
      task:
        description: 'Task for the agent to perform'
        required: true
        type: string
  
  issue_comment:
    types: [created]

jobs:
  run-agent:
    runs-on: ubuntu-latest
    if: github.event_name == 'workflow_dispatch' || (github.event_name == 'issue_comment' && startsWith(github.event.comment.body, '/agent'))
    
    permissions:
      contents: write
      issues: write
      pull-requests: write
    
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: pip install -r requirements.txt
      
      - name: Run agent
        env:
          NIM_API_KEY: ${{ secrets.NIM_API_KEY }}
          HELIUS_API_KEY: ${{ secrets.HELIUS_API_KEY }}
        run: |
          if [ "${{ github.event_name }}" = "workflow_dispatch" ]; then
            python agent/agent.py "${{ inputs.task }}"
          elif [ "${{ github.event_name }}" = "issue_comment" ]; then
            COMMENT="${{ github.event.comment.body }}"
            TASK="${COMMENT#/agent }"
            python agent/agent.py "$TASK"
          fi
      
      - name: Commit changes
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add -A
          git diff --quiet && git diff --staged --quiet || git commit -m "Agent changes from workflow"
          git push || true
      
      - name: Comment on issue
        if: github.event_name == 'issue_comment'
        uses: actions/github-script@v7
        with:
          script: |
            github.rest.issues.createComment({
              issue_number: context.issue.number,
              owner: context.repo.owner,
              repo: context.repo.repo,
              body: '✅ Agent task completed. Check the workflow run for details.'
            });
```

---

## STEP 4: PUSH TO GITHUB

Execute these git commands from D:\files directory:

```bash
# Navigate to repository
cd D:\files

# Initialize git if not already done
git init

# Add remote (replace with your repo URL)
git remote add origin <YOUR_GITHUB_REPO_URL>
# Example: git remote add origin https://github.com/yourusername/nim-qwen-agent.git

# Stage all files
git add .

# Commit
git commit -m "Add Azalyst whale tracking to NIM Qwen Agent"

# Push to GitHub
git push -u origin main
# If main branch doesn't exist, try: git push -u origin master
```

---

## STEP 5: CONFIGURE GITHUB SECRETS

**Navigate to:** `Settings → Secrets and variables → Actions → New repository secret`

Add these secrets:

### Secret 1: NIM_API_KEY
- **Name:** `NIM_API_KEY`
- **Value:** Your NVIDIA NIM API key from https://build.nvidia.com
- **Required:** YES

### Secret 2: HELIUS_API_KEY
- **Name:** `HELIUS_API_KEY`
- **Value:** Your Helius API key from https://Azalyst.so
- **Required:** NO (but recommended for higher rate limits)

**Steps to add secrets:**
1. Go to your repository on GitHub
2. Click `Settings` tab
3. In left sidebar, click `Secrets and variables` → `Actions`
4. Click `New repository secret` button
5. Enter name and value
6. Click `Add secret`
7. Repeat for each secret

---

## STEP 6: ENABLE GITHUB ACTIONS PERMISSIONS

**Navigate to:** `Settings → Actions → General`

**Configure Workflow Permissions:**
1. Scroll to "Workflow permissions" section
2. Select: **"Read and write permissions"**
3. Check: **"Allow GitHub Actions to create and approve pull requests"**
4. Click **"Save"**

This allows the agent to commit changes back to the repository.

---

## STEP 7: VERIFY SETUP

### Test 1: Manual Workflow Trigger

1. Go to `Actions` tab
2. Click `NIM Qwen Agent` workflow
3. Click `Run workflow` button
4. Enter task: `list all Python files in agent/ directory`
5. Click `Run workflow`
6. Wait for completion
7. Check workflow logs

### Test 2: Issue Comment Trigger

1. Go to `Issues` tab
2. Create new issue: "Test agent"
3. Add comment: `/agent find_pumps`
4. Wait for bot response
5. Check workflow run in Actions tab

### Test 3: Whale Tracking Workflow

1. Go to `Actions` tab
2. Check if `Azalyst Whale Tracking Scan` workflow is listed
3. Click `Run workflow` button
4. Select scan type: `daily`
5. Click `Run workflow`
6. Verify it runs successfully

---

## STEP 8: AUTOMATED SCANNING (OPTIONAL)

The whale tracking workflow is configured to run automatically every 4 hours.

**To disable auto-scanning:**
Edit `.github/workflows/whale_tracking.yml` and remove the `schedule:` section.

**To change frequency:**
Edit the cron expression in whale_tracking.yml:
- `0 */4 * * *` = every 4 hours
- `0 */2 * * *` = every 2 hours
- `0 */6 * * *` = every 6 hours
- `0 0 * * *` = daily at midnight

---

## STEP 9: USAGE EXAMPLES

### Via GitHub Actions UI:
```
Actions → NIM Qwen Agent → Run workflow

Example tasks:
- find_pumps
- analyze_token token_address=EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v
- track_whale wallet_address=7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU
- daily_scan
```

### Via Issue Comments:
```
/agent find_pumps
/agent analyze_token token_address=<TOKEN_ADDRESS>
/agent track_whale wallet_address=<WALLET_ADDRESS>
/agent daily_scan
```

### Local Testing (before pushing):
```bash
cd D:\files
export NIM_API_KEY=your_key
export HELIUS_API_KEY=your_key
python agent/agent.py "find_pumps"
```

---

## STEP 10: VERIFICATION CHECKLIST

After completing all steps, verify:

- [ ] All files pushed to GitHub
- [ ] `.github/workflows/` directory exists with 2 workflow files
- [ ] `agent/` directory has 3 Python files (agent.py, tools.py, Azalyst_tracker.py)
- [ ] README.md is updated with Azalyst documentation
- [ ] NIM_API_KEY secret is set
- [ ] HELIUS_API_KEY secret is set (optional)
- [ ] Workflow permissions set to "Read and write"
- [ ] Manual workflow trigger works
- [ ] Issue comment trigger works (`/agent` command)
- [ ] Whale tracking workflow appears in Actions tab

---

## TROUBLESHOOTING

### Issue: Workflow fails with "Permission denied"
**Solution:** Check Step 6 - ensure "Read and write permissions" is enabled

### Issue: "NIM_API_KEY not found"
**Solution:** Check Step 5 - ensure secret is named exactly `NIM_API_KEY`

### Issue: Azalyst tools not working
**Solution:** 
1. Check if Azalyst_tracker.py is in agent/ directory
2. Verify requirements.txt includes `requests>=2.31.0`
3. Run `pip install -r requirements.txt`

### Issue: Git push fails
**Solution:**
```bash
# If branch doesn't exist, create it
git checkout -b main

# Force push (careful!)
git push -f origin main
```

### Issue: Workflow doesn't trigger on comments
**Solution:**
1. Check repository settings → Actions → General
2. Ensure "Allow all actions and reusable workflows" is selected

---

## EXPECTED RESULTS

After successful setup:

1. **Repository Structure:**
   - Clean, organized file structure
   - All necessary files in place
   - Workflows configured and ready

2. **GitHub Actions:**
   - 2 workflows visible in Actions tab
   - Manual triggers working
   - Issue comment triggers working
   - Scheduled runs every 4 hours

3. **Whale Tracking Features:**
   - `find_pumps` - discovers potential pump tokens
   - `analyze_token` - analyzes pump/dump signals
   - `track_whale` - monitors whale wallets
   - `daily_scan` - comprehensive market analysis

4. **Automated Reports:**
   - Scan results saved to reports/ directory
   - Commits pushed automatically
   - Comments posted on issues

---

## NEXT STEPS AFTER SETUP

1. **Test the agent:**
   - Run a few manual workflows
   - Try different Azalyst commands
   - Verify output quality

2. **Monitor automated scans:**
   - Check Actions tab every 4 hours
   - Review generated reports
   - Track whale movements

3. **Customize as needed:**
   - Adjust filters in Azalyst_tracker.py
   - Modify scan frequency
   - Add custom workflows

4. **Share with team:**
   - Show them how to use `/agent` commands
   - Share Azalyst_USAGE.md documentation
   - Train on signal interpretation

---

## SECURITY NOTES

- ✅ API keys stored as GitHub Secrets (encrypted)
- ✅ Never commit API keys to repository
- ✅ Workflow runs in isolated GitHub environment
- ✅ No sensitive data in logs
- ⚠️ Review workflow logs before making public
- ⚠️ Use private repository for sensitive trading data

---

## SUPPORT DOCUMENTATION

After setup, refer to these files:

- **README.md** - Overview and quick start
- **Azalyst_USAGE.md** - Complete Azalyst feature guide
- **AGENTS.md** - Agent behavior and guidelines
- **example_whale_tracking.py** - Local testing examples

---

## COMPLETION CONFIRMATION

When all steps are complete, you should be able to:

1. Go to GitHub Actions
2. Run workflow: "NIM Qwen Agent"
3. Enter task: "daily_scan"
4. See output with whale tracking data
5. Receive automated scans every 4 hours

**Setup is complete when all checkboxes in Step 10 are checked! ✅**

---

## QUICK REFERENCE COMMANDS

```bash
# Local test
python agent/agent.py "find_pumps"

# Standalone test
python example_whale_tracking.py daily

# Push changes
git add . && git commit -m "Update" && git push

# View workflow logs
# Go to: Actions tab → Select workflow run → View logs
```

---

**END OF SETUP INSTRUCTIONS**
