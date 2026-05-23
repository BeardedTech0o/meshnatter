MeshNatter
MeshNatter is a Windows desktop application built to interface with the Meshtastic network. It provides a native, user-friendly bridge between your PC and your Meshtastic hardware, enabling network visualization, real-time node telemetry, and seamless messaging.

Features
IP Connectivity: Connect to Meshtastic nodes directly via TCP/IP or Wi-Fi.

Network Visualization: Integrated map support to visualize the geographical location and signal paths of your mesh nodes.

Messaging Suite: Full support for direct messages and channel broadcasts.

Telemetry Dashboard: Monitor node status, battery levels, and environmental data.

Native Desktop Experience: Built as a Windows native application for performance and seamless system integration.

Prerequisites
A Meshtastic node configured and active on your local network.

The IP address of your node(s).

Windows 11.

Getting Started
Development
To clone the repository and begin working with the source code:

Bash
git clone https://github.com/BeardedTech0o/meshnatter.git
Build Instructions
Open the project in your preferred IDE (e.g., Visual Studio).

Ensure all dependencies are restored.

Build the solution. The application will automatically bundle the required assets located in the /assets directory.

Project Structure
Plaintext
/
├── assets/             # Project icons and tray graphics
├── src/                # Primary source code
├── .gitignore          # Excludes build artifacts (bin, obj, .exe)
├── LICENSE             # Project licensing information
└── README.md           # Documentation
Roadmap
[ ] Implement core Protobuf communication over TCP.

[ ] Finalize UI/UX for node messaging.

[ ] Integrate interactive map rendering.

[ ] Optimize asset management and system tray integration.

Contributing
We welcome contributions! Please fork the repository, make your changes, and open a pull request. For significant architectural changes, please open an issue first to discuss the implementation.

License
This project is licensed under the MIT License. See the LICENSE file for details.
