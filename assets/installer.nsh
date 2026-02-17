!macro customInstall
  SetShellVarContext all
  Delete "$DESKTOP\WhatsConect.lnk"
  CreateShortCut "$DESKTOP\WhatsConect.lnk" "$INSTDIR\WhatsConect Launcher.cmd" "" "$INSTDIR\WhatsConect.exe" 0
  CreateDirectory "$SMPROGRAMS\WhatsConect"
  Delete "$SMPROGRAMS\WhatsConect\WhatsConect.lnk"
  CreateShortCut "$SMPROGRAMS\WhatsConect\WhatsConect.lnk" "$INSTDIR\WhatsConect Launcher.cmd" "" "$INSTDIR\WhatsConect.exe" 0
!macroend
