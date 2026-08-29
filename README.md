# PSE Cloud Functions

[![Platform](https://img.shields.io/badge/Platform-Firebase-FFCA28?logo=firebase&logoColor=black)]()
[![Language](https://img.shields.io/badge/Language-Node.js-339933?logo=nodedotjs&logoColor=white)]()
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> PSE Cloud Functions Repository

---

## 📖 Overview

PSE Cloud Functions Repository

---

## ✨ Key Features

- **computeTeamLevelsAndCreditProfit:** Updates the levels of each team and credits profit to users based on their performance.
- **leadershipBonus:** Calculates and distributes leadership bonuses across eligible accounts.
- **nightlyRoiAndTeam:** Schedules nightly ROI calculations and updates team metrics.
- **salaryProfileScheduler:** Periodically updates salary profiles according to configured schedules.

Each script is an independent Cloud Function deployed to Firebase.

---

## 🛠️ Technology Stack

| Component / Layer | Technology |
|---|---|
| **Platform** | Firebase / GCP |
| **Primary Language** | Node.js |
| **Architecture** | MVVM / Clean Architecture |
| **License** | Open Source (MIT) |

---

## 🚀 Getting Started

1. **Clone the repository**

   ```bash
   git clone https://github.com/shayann07/PSE-Cloud-Functions.git
   cd PSE-Cloud-Functions
   ```

2. **Install dependencies**

   ```bash
   npm install
   ```

3. **Set up Firebase**

   - Make sure you have a Firebase project and the Firebase CLI installed (`npm install -g firebase-tools`).
   - Initialize Firebase in this project if you haven’t already (`firebase init functions`).
   - Update the function configuration and environment variables as needed.

4. **Deploy functions**

   ```bash
   firebase deploy --only functions
   ```

---

## 📄 License

This project is licensed under the [MIT License](LICENSE) — Copyright (c) 2026 [shayann07](https://github.com/shayann07).
