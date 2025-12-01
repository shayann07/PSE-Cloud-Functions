# PSE Cloud Functions

This repository contains the Firebase Cloud Functions that power the **Philippine Stock Exchange (PSE) app**. These Node.js functions handle server side logic such as calculating return on investment (ROI), updating team levels and credit profits, awarding leadership bonuses, and scheduling salary profiles.

## Features

- **computeTeamLevelsAndCreditProfit:** Updates the levels of each team and credits profit to users based on their performance.
- **leadershipBonus:** Calculates and distributes leadership bonuses across eligible accounts.
- **nightlyRoiAndTeam:** Schedules nightly ROI calculations and updates team metrics.
- **salaryProfileScheduler:** Periodically updates salary profiles according to configured schedules.

Each script is an independent Cloud Function deployed to Firebase.

## Getting started

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

## Technologies used

- **Node.js** – JavaScript runtime for building serverless functions.
- **Firebase Cloud Functions** – Serverless execution environment for backend logic.
- **Firebase CLI** – For deploying and managing functions.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
<!-- commit 1 -->
<!-- commit 2 -->
<!-- commit 3 -->
<!-- commit 4 -->
<!-- commit 5 -->
<!-- commit 6 -->
<!-- commit 7 -->
<!-- commit 8 -->
<!-- commit 9 -->
<!-- commit 10 -->
<!-- commit 11 -->
<!-- commit 12 -->
<!-- commit 13 -->
<!-- commit 14 -->
