# GitLab Compliance Checker

A Streamlit web application to verify GitLab project best practices and check user profile README status.

[![Open in Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://glab-compliance-checker.streamlit.app/)

## 🚀 Live Demo

Try the application live: [https://projects-compliance-checker.apps.swecha.org/](https://projects-compliance-checker.apps.swecha.org/)

## 📋 Features

*   **Project Compliance Check**: Analyzes a GitLab project or group for essential files and configurations:
    *   `README.md`
    *   `CONTRIBUTING.md`
    *   `CHANGELOG` (or `CHANGELOG.md`, `CHANGELOG.txt`)
    *   `LICENSE` (or `LICENSE.md`, `LICENSE.txt`)
    *   Issue Templates (in `.gitlab/`)
    *   Merge Request Templates (in `.gitlab/`)
    *   Project Description
    *   Project Tags
*   **User Profile README Check**: Verifies if a GitLab user has set up a profile README by checking for a project named exactly like their username.
*   **Visual Feedback**: Provides clear ✅ or ❌ status for each check.
*   **Actionable Suggestions**: Offers specific guidance and examples (with images) for items that are missing.

## 🛠️ Usage

1.  Visit the live app: [https://glab-compliance-checker.streamlit.app/](https://glab-compliance-checker.streamlit.app/)
2.  Select the mode using the radio button in the sidebar:
    *   **Check Project/Group Compliance**:
        *   Enter the path (e.g., `namespace/project-name`), URL, or ID of the GitLab project or group you want to check.
        *   Click "Check Compliance" or press Enter.
    *   **Check User Profile README**:
        *   Enter a GitLab username, user ID, or user profile URL.
        *   Click "Check README" or press Enter.

## 📦 Requirements (for local development)

*   Python 3.7+
*   Dependencies listed in `requirements.txt`

## ▶️ Run Locally

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/gitlab-compliance-checker.git
    cd gitlab-compliance-checker
    ```
2.  **Create a virtual environment (recommended):**
    ```bash
    python -m venv venv
    # Activate (Linux/macOS)
    source venv/bin/activate
    # Activate (Windows)
    venv\Scripts\activate
    ```
3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
4.  **Set up GitLab credentials:**
    *   Create a `.streamlit/secrets.toml` file in the project root.
    *   Add your GitLab Personal Access Token (with `read_api` scope) and URL:
        ```toml
        GITLAB_TOKEN = "your_gitlab_personal_access_token_here"
        GITLAB_URL = "https://gitlab.com" # Or your GitLab instance URL
        ```
    *   *Note: Do not commit `secrets.toml`. Ensure `.streamlit/secrets.toml` is in your `.gitignore`.*
5.  **Run the app:**
    ```bash
    streamlit run app.py
    ```

## 📄 License

This project is licensed under the GNU License - see the [LICENSE](LICENSE) file for details.
