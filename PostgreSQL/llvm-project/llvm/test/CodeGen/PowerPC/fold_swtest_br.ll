; NOTE: Assertions have been autogenerated by utils/update_llc_test_checks.py
; RUN: llc -verify-machineinstrs -mcpu=pwr9 -mtriple=powerpc64le-unknown-unknown \
; RUN:   -ppc-vsr-nums-as-vr -ppc-asm-full-reg-names < %s | FileCheck %s

@val = external local_unnamed_addr global i32, align 4
declare i32 @llvm.ppc.vsx.xvtsqrtdp(<2 x double>)

define dso_local signext i32 @xvtsqrtdp_and_1_eq(<2 x double> %input) {
; CHECK-LABEL: xvtsqrtdp_and_1_eq:
; CHECK:       # %bb.0: # %entry
; CHECK-NEXT:    xvtsqrtdp cr0, v2
; CHECK-NEXT:    bnu cr0, .LBB0_2
; CHECK-NEXT:  # %bb.1: # %if.then
; CHECK-NEXT:    addis r3, r2, .LC0@toc@ha
; CHECK-NEXT:    li r4, 100
; CHECK-NEXT:    ld r3, .LC0@toc@l(r3)
; CHECK-NEXT:    stw r4, 0(r3)
; CHECK-NEXT:  .LBB0_2: # %if.end
; CHECK-NEXT:    li r3, 1
; CHECK-NEXT:    blr
entry:
  %0 = tail call i32 @llvm.ppc.vsx.xvtsqrtdp(<2 x double> %input)
  %1 = and i32 %0, 1
  %cmp.not = icmp eq i32 %1, 0
  br i1 %cmp.not, label %if.end, label %if.then

if.then:                                          ; preds = %entry
  store i32 100, i32* @val, align 4
  br label %if.end

if.end:                                           ; preds = %if.then, %entry
  ret i32 1
}

define dso_local signext i32 @xvtsqrtdp_and_2_eq(<2 x double> %input) {
; CHECK-LABEL: xvtsqrtdp_and_2_eq:
; CHECK:       # %bb.0: # %entry
; CHECK-NEXT:    xvtsqrtdp cr0, v2
; CHECK-NEXT:    bne cr0, .LBB1_2
; CHECK-NEXT:  # %bb.1: # %if.then
; CHECK-NEXT:    addis r3, r2, .LC0@toc@ha
; CHECK-NEXT:    li r4, 100
; CHECK-NEXT:    ld r3, .LC0@toc@l(r3)
; CHECK-NEXT:    stw r4, 0(r3)
; CHECK-NEXT:  .LBB1_2: # %if.end
; CHECK-NEXT:    li r3, 1
; CHECK-NEXT:    blr
entry:
  %0 = tail call i32 @llvm.ppc.vsx.xvtsqrtdp(<2 x double> %input)
  %1 = and i32 %0, 2
  %cmp.not = icmp eq i32 %1, 0
  br i1 %cmp.not, label %if.end, label %if.then

if.then:                                          ; preds = %entry
  store i32 100, i32* @val, align 4
  br label %if.end

if.end:                                           ; preds = %if.then, %entry
  ret i32 1
}

define dso_local signext i32 @xvtsqrtdp_and_4_eq(<2 x double> %input) {
; CHECK-LABEL: xvtsqrtdp_and_4_eq:
; CHECK:       # %bb.0: # %entry
; CHECK-NEXT:    xvtsqrtdp cr0, v2
; CHECK-NEXT:    ble cr0, .LBB2_2
; CHECK-NEXT:  # %bb.1: # %if.then
; CHECK-NEXT:    addis r3, r2, .LC0@toc@ha
; CHECK-NEXT:    li r4, 100
; CHECK-NEXT:    ld r3, .LC0@toc@l(r3)
; CHECK-NEXT:    stw r4, 0(r3)
; CHECK-NEXT:  .LBB2_2: # %if.end
; CHECK-NEXT:    li r3, 1
; CHECK-NEXT:    blr
entry:
  %0 = tail call i32 @llvm.ppc.vsx.xvtsqrtdp(<2 x double> %input)
  %1 = and i32 %0, 4
  %cmp.not = icmp eq i32 %1, 0
  br i1 %cmp.not, label %if.end, label %if.then

if.then:                                          ; preds = %entry
  store i32 100, i32* @val, align 4
  br label %if.end

if.end:                                           ; preds = %if.then, %entry
  ret i32 1
}

define dso_local signext i32 @xvtsqrtdp_and_8_eq(<2 x double> %input) {
; CHECK-LABEL: xvtsqrtdp_and_8_eq:
; CHECK:       # %bb.0: # %entry
; CHECK-NEXT:    xvtsqrtdp cr0, v2
; CHECK-NEXT:    bge cr0, .LBB3_2
; CHECK-NEXT:  # %bb.1: # %if.then
; CHECK-NEXT:    addis r3, r2, .LC0@toc@ha
; CHECK-NEXT:    li r4, 100
; CHECK-NEXT:    ld r3, .LC0@toc@l(r3)
; CHECK-NEXT:    stw r4, 0(r3)
; CHECK-NEXT:  .LBB3_2: # %if.end
; CHECK-NEXT:    li r3, 1
; CHECK-NEXT:    blr
entry:
  %0 = tail call i32 @llvm.ppc.vsx.xvtsqrtdp(<2 x double> %input)
  %1 = and i32 %0, 8
  %cmp.not = icmp eq i32 %1, 0
  br i1 %cmp.not, label %if.end, label %if.then

if.then:                                          ; preds = %entry
  store i32 100, i32* @val, align 4
  br label %if.end

if.end:                                           ; preds = %if.then, %entry
  ret i32 1
}

define dso_local signext i32 @xvtsqrtdp_and_1_ne(<2 x double> %input) {
; CHECK-LABEL: xvtsqrtdp_and_1_ne:
; CHECK:       # %bb.0: # %entry
; CHECK-NEXT:    xvtsqrtdp cr0, v2
; CHECK-NEXT:    bun cr0, .LBB4_2
; CHECK-NEXT:  # %bb.1: # %if.then
; CHECK-NEXT:    addis r3, r2, .LC0@toc@ha
; CHECK-NEXT:    li r4, 100
; CHECK-NEXT:    ld r3, .LC0@toc@l(r3)
; CHECK-NEXT:    stw r4, 0(r3)
; CHECK-NEXT:  .LBB4_2: # %if.end
; CHECK-NEXT:    li r3, 1
; CHECK-NEXT:    blr
entry:
  %0 = tail call i32 @llvm.ppc.vsx.xvtsqrtdp(<2 x double> %input)
  %1 = and i32 %0, 1
  %cmp.not = icmp ne i32 %1, 0
  br i1 %cmp.not, label %if.end, label %if.then

if.then:                                          ; preds = %entry
  store i32 100, i32* @val, align 4
  br label %if.end

if.end:                                           ; preds = %if.then, %entry
  ret i32 1
}

define dso_local signext i32 @xvtsqrtdp_and_2_ne(<2 x double> %input) {
; CHECK-LABEL: xvtsqrtdp_and_2_ne:
; CHECK:       # %bb.0: # %if.end
; CHECK-NEXT:    li r3, 1
; CHECK-NEXT:    blr
if.end:                                           ; preds = %if.then, %entry
  ret i32 1
}

define dso_local signext i32 @xvtsqrtdp_and_4_ne(<2 x double> %input) {
; CHECK-LABEL: xvtsqrtdp_and_4_ne:
; CHECK:       # %bb.0: # %entry
; CHECK-NEXT:    xvtsqrtdp cr0, v2
; CHECK-NEXT:    bgt cr0, .LBB6_2
; CHECK-NEXT:  # %bb.1: # %if.then
; CHECK-NEXT:    addis r3, r2, .LC0@toc@ha
; CHECK-NEXT:    li r4, 100
; CHECK-NEXT:    ld r3, .LC0@toc@l(r3)
; CHECK-NEXT:    stw r4, 0(r3)
; CHECK-NEXT:  .LBB6_2: # %if.end
; CHECK-NEXT:    li r3, 1
; CHECK-NEXT:    blr
entry:
  %0 = tail call i32 @llvm.ppc.vsx.xvtsqrtdp(<2 x double> %input)
  %1 = and i32 %0, 4
  %cmp.not = icmp ne i32 %1, 0
  br i1 %cmp.not, label %if.end, label %if.then

if.then:                                          ; preds = %entry
  store i32 100, i32* @val, align 4
  br label %if.end

if.end:                                           ; preds = %if.then, %entry
  ret i32 1
}

define dso_local signext i32 @xvtsqrtdp_and_8_ne(<2 x double> %input) {
; CHECK-LABEL: xvtsqrtdp_and_8_ne:
; CHECK:       # %bb.0: # %entry
; CHECK-NEXT:    xvtsqrtdp cr0, v2
; CHECK-NEXT:    blt cr0, .LBB7_2
; CHECK-NEXT:  # %bb.1: # %if.then
; CHECK-NEXT:    addis r3, r2, .LC0@toc@ha
; CHECK-NEXT:    li r4, 100
; CHECK-NEXT:    ld r3, .LC0@toc@l(r3)
; CHECK-NEXT:    stw r4, 0(r3)
; CHECK-NEXT:  .LBB7_2: # %if.end
; CHECK-NEXT:    li r3, 1
; CHECK-NEXT:    blr
entry:
  %0 = tail call i32 @llvm.ppc.vsx.xvtsqrtdp(<2 x double> %input)
  %1 = and i32 %0, 8
  %cmp.not = icmp ne i32 %1, 0
  br i1 %cmp.not, label %if.end, label %if.then

if.then:                                          ; preds = %entry
  store i32 100, i32* @val, align 4
  br label %if.end

if.end:                                           ; preds = %if.then, %entry
  ret i32 1
}