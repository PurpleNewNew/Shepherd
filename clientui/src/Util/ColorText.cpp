#include <Util/ColorText.h>

QString StockmanNamespace::Util::ColorText::Colors::Hex::Background    = "#282a36";
QString StockmanNamespace::Util::ColorText::Colors::Hex::Foreground    = "#f8f8f2";
QString StockmanNamespace::Util::ColorText::Colors::Hex::Comment       = "#6272a4";
QString StockmanNamespace::Util::ColorText::Colors::Hex::CurrentLine   = "#44475a";

QString StockmanNamespace::Util::ColorText::Colors::Hex::Cyan          = "#8be9fd";
QString StockmanNamespace::Util::ColorText::Colors::Hex::Green         = "#50fa7b";
QString StockmanNamespace::Util::ColorText::Colors::Hex::Orange        = "#ffb86c";
QString StockmanNamespace::Util::ColorText::Colors::Hex::Pink          = "#ff79c6";
QString StockmanNamespace::Util::ColorText::Colors::Hex::Purple        = "#bd93f9";
QString StockmanNamespace::Util::ColorText::Colors::Hex::Red           = "#ff5555";
QString StockmanNamespace::Util::ColorText::Colors::Hex::Yellow        = "#f1fa8c";

QString StockmanNamespace::Util::ColorText::Colors::Hex::SessionCyan   = "#618bac";
QString StockmanNamespace::Util::ColorText::Colors::Hex::SessionGreen  = "#1C5F11";
QString StockmanNamespace::Util::ColorText::Colors::Hex::SessionOrange = "#ac7420";
QString StockmanNamespace::Util::ColorText::Colors::Hex::SessionPink   = "#c33fb6";
QString StockmanNamespace::Util::ColorText::Colors::Hex::SessionPurple = "#36365b";
QString StockmanNamespace::Util::ColorText::Colors::Hex::SessionRed    = "#5b3d3e";
QString StockmanNamespace::Util::ColorText::Colors::Hex::SessionYellow = "#a59220";

void StockmanNamespace::Util::ColorText::SetDraculaDark()
{
    StockmanNamespace::Util::ColorText::Colors::Hex::Background    = "#282a36";
    StockmanNamespace::Util::ColorText::Colors::Hex::Foreground    = "#f8f8f2";
    StockmanNamespace::Util::ColorText::Colors::Hex::Comment       = "#6272a4";
    StockmanNamespace::Util::ColorText::Colors::Hex::CurrentLine   = "#44475a";

    StockmanNamespace::Util::ColorText::Colors::Hex::Cyan          = "#8be9fd";
    StockmanNamespace::Util::ColorText::Colors::Hex::Green         = "#50fa7b";
    StockmanNamespace::Util::ColorText::Colors::Hex::Orange        = "#ffb86c";
    StockmanNamespace::Util::ColorText::Colors::Hex::Pink          = "#ff79c6";
    StockmanNamespace::Util::ColorText::Colors::Hex::Purple        = "#bd93f9";
    StockmanNamespace::Util::ColorText::Colors::Hex::Red           = "#ff5555";
    StockmanNamespace::Util::ColorText::Colors::Hex::Yellow        = "#f1fa8c";
}

void StockmanNamespace::Util::ColorText::SetDraculaLight()
{
    StockmanNamespace::Util::ColorText::Colors::Hex::Background    = "#f8f8f2";
    StockmanNamespace::Util::ColorText::Colors::Hex::Foreground    = "#282a36";
    StockmanNamespace::Util::ColorText::Colors::Hex::Comment       = "#707070";
    StockmanNamespace::Util::ColorText::Colors::Hex::CurrentLine   = "#e6e6e6";

    StockmanNamespace::Util::ColorText::Colors::Hex::Cyan          = "#1597e5";
    StockmanNamespace::Util::ColorText::Colors::Hex::Green         = "#2da44e";
    StockmanNamespace::Util::ColorText::Colors::Hex::Orange        = "#e69500";
    StockmanNamespace::Util::ColorText::Colors::Hex::Pink          = "#d6336c";
    StockmanNamespace::Util::ColorText::Colors::Hex::Purple        = "#7c3aed";
    StockmanNamespace::Util::ColorText::Colors::Hex::Red           = "#d11a2a";
    StockmanNamespace::Util::ColorText::Colors::Hex::Yellow        = "#c9a600";

    StockmanNamespace::Util::ColorText::Colors::Hex::SessionCyan   = "#3a7ca5";
    StockmanNamespace::Util::ColorText::Colors::Hex::SessionGreen  = "#1c7c54";
    StockmanNamespace::Util::ColorText::Colors::Hex::SessionOrange = "#c47f17";
    StockmanNamespace::Util::ColorText::Colors::Hex::SessionPink   = "#b83280";
    StockmanNamespace::Util::ColorText::Colors::Hex::SessionPurple = "#5c4b8a";
    StockmanNamespace::Util::ColorText::Colors::Hex::SessionRed    = "#8c2f39";
    StockmanNamespace::Util::ColorText::Colors::Hex::SessionYellow = "#9c8a00";
}

QString StockmanNamespace::Util::ColorText::Color(const QString& color, const QString &text)
{
    return "<span style=\"color: "+ color +";\" >" + text.toHtmlEscaped() + "</span>";
}

QString StockmanNamespace::Util::ColorText::Background(const QString& text)
{
    return "<span style=\"color: "+ StockmanNamespace::Util::ColorText::Colors::Hex::Background +";\" >" + text.toHtmlEscaped() + "</span>";
}

QString StockmanNamespace::Util::ColorText::Foreground(const QString& text) {
    return "<span style=\"color: "+ StockmanNamespace::Util::ColorText::Colors::Hex::Foreground +";\" >" + text.toHtmlEscaped() + "</span>";
}

QString StockmanNamespace::Util::ColorText::Comment(const QString& text) {
    return "<span style=\"color: "+ StockmanNamespace::Util::ColorText::Colors::Hex::Comment +";\" >" + text.toHtmlEscaped() + "</span>";
}

QString StockmanNamespace::Util::ColorText::Cyan(const QString& text) {
    return "<span style=\"color: "+ StockmanNamespace::Util::ColorText::Colors::Hex::Cyan +";\" >" + text.toHtmlEscaped() + "</span>";
}

QString StockmanNamespace::Util::ColorText::Green(const QString& text) {
    return "<span style=\"color: "+ StockmanNamespace::Util::ColorText::Colors::Hex::Green +";\" >" + text.toHtmlEscaped() + "</span>";
}

QString StockmanNamespace::Util::ColorText::Orange(const QString& text) {
    return "<span style=\"color: "+ StockmanNamespace::Util::ColorText::Colors::Hex::Orange +";\" >" + text.toHtmlEscaped() + "</span>";
}

QString StockmanNamespace::Util::ColorText::Pink(const QString& text) {
    return "<span style=\"color: "+ StockmanNamespace::Util::ColorText::Colors::Hex::Pink +";\" >" + text.toHtmlEscaped() + "</span>";
}

QString StockmanNamespace::Util::ColorText::Purple(const QString& text) {
    return "<span style=\"color: "+ StockmanNamespace::Util::ColorText::Colors::Hex::Purple +";\" >" + text.toHtmlEscaped() + "</span>";
}

QString StockmanNamespace::Util::ColorText::Red(const QString& text) {
    return "<span style=\"color: "+ StockmanNamespace::Util::ColorText::Colors::Hex::Red +";\" >" + text.toHtmlEscaped() + "</span>";
}

QString StockmanNamespace::Util::ColorText::Yellow(const QString& text) {
    return "<span style=\"color: "+ StockmanNamespace::Util::ColorText::Colors::Hex::Yellow +";\" >" + text.toHtmlEscaped() + "</span>";
}

QString StockmanNamespace::Util::ColorText::Bold(const QString& text) {
    return "<b>" + text.toHtmlEscaped() + "</b>";
}

QString StockmanNamespace::Util::ColorText::Underline(const QString &text) {
    return "<span style=\"text-decoration:underline\">" + text.toHtmlEscaped() + "</span>";
}

QString StockmanNamespace::Util::ColorText::UnderlineBackground(const QString &text) {
    return "<span style=\"text-decoration:underline; color: "+ StockmanNamespace::Util::ColorText::Colors::Hex::Background +";\" >" + text.toHtmlEscaped() + "</span>";
}

QString StockmanNamespace::Util::ColorText::UnderlineForeground(const QString &text) {
    return "<span style=\"text-decoration:underline; color: "+ StockmanNamespace::Util::ColorText::Colors::Hex::Foreground +";\" >" + text.toHtmlEscaped() + "</span>";
}

QString StockmanNamespace::Util::ColorText::UnderlineComment(const QString &text) {
    return "<span style=\"text-decoration:underline; color: "+ StockmanNamespace::Util::ColorText::Colors::Hex::Comment +";\" >" + text.toHtmlEscaped() + "</span>";
}

QString StockmanNamespace::Util::ColorText::UnderlineCyan(const QString &text) {
    return "<span style=\"text-decoration:underline; color: "+ StockmanNamespace::Util::ColorText::Colors::Hex::Cyan +";\" >" + text.toHtmlEscaped() + "</span>";
}

QString StockmanNamespace::Util::ColorText::UnderlineGreen(const QString &text) {
    return "<span style=\"text-decoration:underline; color: "+ StockmanNamespace::Util::ColorText::Colors::Hex::Green +";\" >" + text.toHtmlEscaped() + "</span>";
}

QString StockmanNamespace::Util::ColorText::UnderlineOrange(const QString &text) {
    return "<span style=\"text-decoration:underline; color: "+ StockmanNamespace::Util::ColorText::Colors::Hex::Orange +";\" >" + text.toHtmlEscaped() + "</span>";
}

QString StockmanNamespace::Util::ColorText::UnderlinePink(const QString &text) {
    return "<span style=\"text-decoration:underline; color: "+ StockmanNamespace::Util::ColorText::Colors::Hex::Pink +";\" >" + text.toHtmlEscaped() + "</span>";
}

QString StockmanNamespace::Util::ColorText::UnderlinePurple(const QString &text) {
    return "<span style=\"text-decoration:underline; color: "+ StockmanNamespace::Util::ColorText::Colors::Hex::Purple +";\" >" + text.toHtmlEscaped() + "</span>";
}

QString StockmanNamespace::Util::ColorText::UnderlineRed(const QString &text) {
    return "<span style=\"text-decoration:underline; color: "+ StockmanNamespace::Util::ColorText::Colors::Hex::Red +";\" >" + text.toHtmlEscaped() + "</span>";
}

QString StockmanNamespace::Util::ColorText::UnderlineYellow(const QString &text) {
    return "<span style=\"text-decoration:underline; color: "+ StockmanNamespace::Util::ColorText::Colors::Hex::Yellow +";\" >" + text.toHtmlEscaped() + "</span>";
}
