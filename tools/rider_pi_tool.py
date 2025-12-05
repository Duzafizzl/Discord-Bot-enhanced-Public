"""
🤖 RIDER PI TOOL - Demo-Bewegungen für den Rider Pi Robot
==========================================================

Dieses Tool ermöglicht Letta, die Predefined Demo-Bewegungen des Rider Pi
auszuführen. Die Kommunikation läuft über Discord → Discord Bot → SSH → Rider Pi.

EINZELNE ACTION:
----------------
  rider_pi_tool(action="happy_dance")
  rider_pi_tool(action="adjust_height", height=100)

SEQUENZ/BATCH (mehrere Actions hintereinander):
-----------------------------------------------
  rider_pi_tool(
      action="sequence",
      sequence=[
          {"action": "happy_dance"},
          {"action": "adjust_height", "height": 100},
          {"action": "periodic_shake", "period": 1.5},
          {"action": "reset"}
      ],
      delay_between=2.0  # 2 Sekunden Pause zwischen Actions
  )

VERFÜGBARE ACTIONS:
-------------------
Demo-Bewegungen (Action 1-6):
  • glueckliches_wackeln    - Fröhliche Wackel-Bewegung
  • auf_und_ab_wackeln      - Vertikale Wackel-Bewegung  
  • vor_und_zurueck_rollen  - Kurze Vor- und Rückwärtsbewegung
  • achten_fahren           - Fahrt in Form einer Acht
  • kreis_drehen            - Auf und Ab im Kreis drehen
  • happy_dance             - Fröhlicher Tanz

Erweiterte Steuerung:
  • adjust_height     - Höhe anpassen (75-115mm)
  • adjust_roll       - Seitliche Neigung (-17 bis 17°)
  • balance_mode      - Selbststabilisierung an/aus
  • periodic_squat    - Periodische Kniebeugen (0-4s)
  • periodic_shake    - Periodisches Wackeln (0-4s)
  • reset             - Zurück auf Standard-Position
  • test_connection   - SSH-Verbindung testen

Batch/Sequenz:
  • sequence          - Führt mehrere Actions nacheinander aus
  • list_actions      - Zeigt alle verfügbaren Actions
"""

import requests
import json
import time
import os
import sys

# ==================== CONFIGURATION ====================

DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")

# MCP Command Channel from environment
MCP_COMMAND_CHANNEL_ID = os.getenv("MCP_COMMAND_CHANNEL_ID", "")

# Timeout für Bot-Antwort (Sekunden)
COMMAND_TIMEOUT = int(os.getenv("MCP_COMMAND_TIMEOUT", "60"))

# ==================== ACTION MAPPING ====================

ACTION_MAP = {
    # === Demo-Bewegungen (Action 1-6) ===
    "glueckliches_wackeln": {
        "tool": "rider_pi_glueckliches_wackeln",
        "args": {},
        "description": "Fröhliche Wackel-Bewegung (Action 1)"
    },
    "auf_und_ab_wackeln": {
        "tool": "rider_pi_auf_und_ab_wackeln", 
        "args": {},
        "description": "Vertikale Wackel-Bewegung (Action 2)"
    },
    "vor_und_zurueck_rollen": {
        "tool": "rider_pi_kurz_vor_und_zurueck_rollen",
        "args": {},
        "description": "Kurze Vor- und Rückwärtsbewegung (Action 3)"
    },
    "achten_fahren": {
        "tool": "rider_pi_achten_fahren",
        "args": {},
        "description": "Fahrt in Form einer Acht (Action 4)"
    },
    "kreis_drehen": {
        "tool": "rider_pi_auf_und_ab_im_kreisdrehen",
        "args": {},
        "description": "Auf und Ab im Kreis drehen (Action 5)"
    },
    "happy_dance": {
        "tool": "rider_pi_happy_dance",
        "args": {},
        "description": "Fröhlicher Tanz (Action 6)"
    },
    
    # === Erweiterte Steuerung ===
    "adjust_height": {
        "tool": "rider_pi_adjust_height",
        "args": {"height": 85},
        "description": "Höhe anpassen (75-115mm)",
        "params": ["height"]
    },
    "adjust_roll": {
        "tool": "rider_pi_adjust_roll",
        "args": {"roll": 0},
        "description": "Seitliche Neigung (-17 bis 17°)",
        "params": ["roll"]
    },
    "balance_mode": {
        "tool": "rider_pi_set_balance_mode",
        "args": {"enabled": True},
        "description": "Selbststabilisierung an/aus",
        "params": ["enabled"]
    },
    "periodic_squat": {
        "tool": "rider_pi_periodic_squat",
        "args": {"period": 0.0},
        "description": "Periodische Kniebeugen (0-4s, 0=stop)",
        "params": ["period"]
    },
    "periodic_shake": {
        "tool": "rider_pi_periodic_shake",
        "args": {"period": 0.0},
        "description": "Periodisches Wackeln (0-4s, 0=stop)",
        "params": ["period"]
    },
    "reset": {
        "tool": "rider_pi_reset",
        "args": {},
        "description": "Zurück auf Standard-Position"
    },
    "test_connection": {
        "tool": "rider_pi_test_connection",
        "args": {},
        "description": "SSH-Verbindung zum Rider Pi testen"
    }
}

# ==================== MAIN TOOL FUNCTION ====================

def rider_pi_tool(
    action: str,
    # Parameter für erweiterte Steuerung
    height: int = None,      # adjust_height: 75-115mm
    roll: int = None,        # adjust_roll: -17 bis 17°
    enabled: bool = None,    # balance_mode: True/False
    period: float = None,    # periodic_squat/shake: 0.0-4.0s
    # Batch/Sequenz Parameter
    sequence: list = None,   # Liste von Actions für "sequence"
    delay_between: float = 1.0  # Pause zwischen Actions in Sekunden
) -> dict:
    """
    🤖 Rider Pi Demo-Bewegungen Tool
    
    Steuert den Rider Pi Roboter über Discord Bot Bridge.
    Kann einzelne Actions oder ganze Sequenzen ausführen.
    
    Args:
        action: Die auszuführende Aktion:
            Demo-Bewegungen:
              • glueckliches_wackeln - Fröhliche Wackel-Bewegung
              • auf_und_ab_wackeln - Vertikale Wackel-Bewegung
              • vor_und_zurueck_rollen - Kurze Vor- und Rückwärtsbewegung
              • achten_fahren - Fahrt in Form einer Acht
              • kreis_drehen - Auf und Ab im Kreis drehen
              • happy_dance - Fröhlicher Tanz
            
            Erweiterte Steuerung:
              • adjust_height - Höhe anpassen (braucht height Parameter)
              • adjust_roll - Seitliche Neigung (braucht roll Parameter)
              • balance_mode - Selbststabilisierung (braucht enabled Parameter)
              • periodic_squat - Periodische Kniebeugen (braucht period Parameter)
              • periodic_shake - Periodisches Wackeln (braucht period Parameter)
              • reset - Zurück auf Standard-Position
              • test_connection - SSH-Verbindung testen
            
            Batch/Sequenz:
              • sequence - Führt mehrere Actions nacheinander aus
              • list_actions - Zeigt alle verfügbaren Actions
        
        height: Höhe in mm (75-115) für adjust_height
        roll: Roll-Winkel in Grad (-17 bis 17) für adjust_roll
        enabled: True/False für balance_mode
        period: Periodendauer in Sekunden (0.0-4.0) für periodic_squat/shake
        
        sequence: Liste von Action-Dicts für "sequence" action, z.B.:
            [
                {"action": "happy_dance"},
                {"action": "adjust_height", "height": 100},
                {"action": "periodic_shake", "period": 1.5},
                {"action": "reset"}
            ]
        delay_between: Pause zwischen Sequenz-Actions in Sekunden (Standard: 1.0)
    
    Returns:
        dict mit status, message, und optional results
    """
    
    # === Special: list_actions ===
    if action == "list_actions":
        return {
            "status": "success",
            "message": "Verfügbare Actions:",
            "actions": {name: info["description"] for name, info in ACTION_MAP.items()},
            "example_sequence": [
                {"action": "happy_dance"},
                {"action": "adjust_height", "height": 100},
                {"action": "reset"}
            ]
        }
    
    # === Special: sequence (Batch-Ausführung) ===
    if action == "sequence":
        if not sequence or not isinstance(sequence, list):
            return {
                "status": "error",
                "message": "Für 'sequence' wird eine Liste von Actions benötigt. Beispiel: sequence=[{'action': 'happy_dance'}, {'action': 'reset'}]"
            }
        
        return _execute_sequence(sequence, delay_between)
    
    # === Einzelne Action ausführen ===
    return _execute_single_action(action, height, roll, enabled, period)

# ==================== SEQUENCE EXECUTION ====================

def _execute_sequence(sequence: list, delay_between: float) -> dict:
    """Führt eine Sequenz von Actions nacheinander aus."""
    
    results = []
    total_actions = len(sequence)
    successful = 0
    failed = 0
    
    print(f"🎬 Starte Sequenz mit {total_actions} Actions...")
    sys.stdout.flush()
    
    for i, action_dict in enumerate(sequence):
        action = action_dict.get("action")
        
        if not action:
            results.append({
                "index": i,
                "action": "unknown",
                "status": "error",
                "message": "Keine 'action' im Dict gefunden"
            })
            failed += 1
            continue
        
        # Extrahiere Parameter
        height = action_dict.get("height")
        roll = action_dict.get("roll")
        enabled = action_dict.get("enabled")
        period = action_dict.get("period")
        
        # Progress anzeigen
        progress_pct = ((i + 1) / total_actions) * 100
        print(f"[{i+1}/{total_actions}] ({progress_pct:.1f}%) Führe '{action}' aus...")
        sys.stdout.flush()
        
        # Action ausführen
        result = _execute_single_action(action, height, roll, enabled, period)
        result["index"] = i
        result["action"] = action
        results.append(result)
        
        if result.get("status") == "success":
            successful += 1
            print(f"  ✅ {action} erfolgreich")
        else:
            failed += 1
            print(f"  ❌ {action} fehlgeschlagen: {result.get('message', 'Unknown error')}")
        
        sys.stdout.flush()
        
        # Pause zwischen Actions (außer bei letzter)
        if i < total_actions - 1 and delay_between > 0:
            print(f"  ⏳ Warte {delay_between}s...")
            sys.stdout.flush()
            time.sleep(delay_between)
    
    # Zusammenfassung
    summary = f"Sequenz abgeschlossen: {successful}/{total_actions} erfolgreich"
    if failed > 0:
        summary += f", {failed} fehlgeschlagen"
    
    print(f"🏁 {summary}")
    sys.stdout.flush()
    
    return {
        "status": "success" if failed == 0 else "partial",
        "message": summary,
        "total_actions": total_actions,
        "successful": successful,
        "failed": failed,
        "results": results
    }

# ==================== SINGLE ACTION EXECUTION ====================

def _execute_single_action(action: str, height: int = None, roll: int = None, 
                           enabled: bool = None, period: float = None) -> dict:
    """Führt eine einzelne Action aus."""
    
    # Validiere Action
    if action not in ACTION_MAP:
        available = ", ".join(ACTION_MAP.keys())
        return {
            "status": "error",
            "message": f"Unbekannte Action: '{action}'. Verfügbar: {available}"
        }
    
    # Hole Mapping
    mapping = ACTION_MAP[action].copy()
    tool_name = mapping["tool"]
    args = mapping.get("args", {}).copy()
    
    # Füge Parameter hinzu falls angegeben
    if action == "adjust_height" and height is not None:
        args["height"] = max(75, min(115, int(height)))
    elif action == "adjust_roll" and roll is not None:
        args["roll"] = max(-17, min(17, int(roll)))
    elif action == "balance_mode" and enabled is not None:
        args["enabled"] = bool(enabled)
    elif action in ("periodic_squat", "periodic_shake") and period is not None:
        args["period"] = max(0.0, min(4.0, float(period)))
    
    # Baue MCP Command
    request_id = f"mcp_{int(time.time() * 1000)}"
    mcp_command = {
        "type": "mcp_command",
        "tool": tool_name,
        "arguments": args,
        "timestamp": time.time(),
        "request_id": request_id
    }
    
    # Sende über Discord
    try:
        result = _send_mcp_command(mcp_command, request_id)
        return result
    except Exception as e:
        return {"status": "error", "message": f"Exception: {str(e)}"}

# ==================== DISCORD COMMUNICATION ====================

def _send_mcp_command(mcp_command: dict, request_id: str) -> dict:
    """Sendet MCP Command über Discord und wartet auf Antwort."""
    
    headers = {
        "Authorization": f"Bot {DISCORD_BOT_TOKEN}",
        "Content-Type": "application/json"
    }
    
    # Formatiere Message
    msg_content = f"🤖 MCP_COMMAND [{request_id}]: ```json\n{json.dumps(mcp_command, indent=2)}\n```"
    
    # Sende Command
    try:
        response = requests.post(
            f"https://discord.com/api/v10/channels/{MCP_COMMAND_CHANNEL_ID}/messages",
            headers=headers,
            json={"content": msg_content},
            timeout=10
        )
        
        if response.status_code not in (200, 201):
            return {
                "status": "error",
                "message": f"Discord API Error: {response.status_code} - {response.text}"
            }
        
        command_msg_id = response.json()["id"]
        
    except requests.exceptions.RequestException as e:
        return {"status": "error", "message": f"Netzwerk-Fehler: {str(e)}"}
    
    # Warte auf Antwort
    return _wait_for_response(headers, command_msg_id, request_id)

def _wait_for_response(headers: dict, command_msg_id: str, request_id: str) -> dict:
    """Wartet auf Bot-Antwort."""
    
    start_time = time.time()
    
    while time.time() - start_time < COMMAND_TIMEOUT:
        time.sleep(1.5)
        
        try:
            response = requests.get(
                f"https://discord.com/api/v10/channels/{MCP_COMMAND_CHANNEL_ID}/messages",
                headers=headers,
                params={"limit": 10, "after": command_msg_id},
                timeout=10
            )
            
            if response.status_code != 200:
                continue
            
            messages = response.json()
            
            for msg in messages:
                content = msg.get("content", "")
                
                # Suche nach Response mit unserer request_id
                if f"MCP_RESPONSE [{request_id}]" in content:
                    try:
                        # Extrahiere JSON
                        if "```json" in content:
                            json_start = content.find("```json") + 7
                            json_end = content.find("```", json_start)
                            result_json = content[json_start:json_end].strip()
                            return json.loads(result_json)
                    except json.JSONDecodeError:
                        return {
                            "status": "success",
                            "message": "Command ausgeführt",
                            "raw_response": content
                        }
                
                # Fehler-Response
                if f"MCP_ERROR [{request_id}]" in content:
                    return {"status": "error", "message": content}
        
        except requests.exceptions.RequestException:
            continue
    
    return {
        "status": "error", 
        "message": f"Timeout nach {COMMAND_TIMEOUT}s. Der Command wird möglicherweise noch ausgeführt."
    }


# ==================== TEST ====================

if __name__ == "__main__":
    print("🤖 Rider Pi Tool - Test Mode")
    print("=" * 50)
    
    print("\n📋 Verfügbare Actions:")
    for name, info in ACTION_MAP.items():
        print(f"  • {name}: {info['description']}")
    
    print("\n📋 Beispiel-Sequenz:")
    example = [
        {"action": "happy_dance"},
        {"action": "adjust_height", "height": 100},
        {"action": "periodic_shake", "period": 1.5},
        {"action": "reset"}
    ]
    print(f"  {json.dumps(example, indent=2)}")
